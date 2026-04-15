#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'yaml'
require 'set'
require 'optparse'

module JsonSchemaCompatibilityGuard
  VERSION = '1.0.0'
  COMPATIBLE_EXIT = 0
  INCOMPATIBLE_EXIT = 1
  ERROR_EXIT = 2

  ANNOTATION_KEYS = Set.new(%w[
    $schema
    $id
    title
    description
    examples
    example
    default
    deprecated
    readOnly
    writeOnly
    $comment
  ]).freeze

  UNSUPPORTED_COMPARISON_KEYWORDS = %w[
    anyOf
    oneOf
    not
    if
    then
    else
    dependentSchemas
    unevaluatedProperties
    unevaluatedItems
    contentSchema
  ].freeze

  class Error < StandardError; end

  Issue = Struct.new(:severity, :path, :message, :details, keyword_init: true) do
    def to_h
      payload = {
        severity: severity.to_s,
        path: path,
        message: message
      }
      payload[:details] = details if details && !details.empty?
      payload
    end
  end

  class Report
    attr_reader :mode, :source_path, :candidate_path, :issues

    def initialize(mode:, source_path:, candidate_path:)
      @mode = mode
      @source_path = source_path
      @candidate_path = candidate_path
      @issues = []
    end

    def add(severity, path, message, details = nil)
      @issues << Issue.new(severity: severity, path: normalize_path(path), message: message, details: details)
    end

    def breaking(path, message, details = nil)
      add(:breaking, path, message, details)
    end

    def warning(path, message, details = nil)
      add(:warning, path, message, details)
    end

    def note(path, message, details = nil)
      add(:note, path, message, details)
    end

    def breaking?
      @issues.any? { |issue| issue.severity == :breaking }
    end

    def warning?
      @issues.any? { |issue| issue.severity == :warning }
    end

    def compatible?
      !breaking?
    end

    def exit_code(fail_on)
      return INCOMPATIBLE_EXIT if fail_on == 'warning' && (breaking? || warning?)
      return INCOMPATIBLE_EXIT if breaking?

      COMPATIBLE_EXIT
    end

    def to_h
      {
        mode: mode,
        source_schema: source_path,
        candidate_schema: candidate_path,
        rule: rule_text,
        compatible: compatible?,
        counts: {
          breaking: @issues.count { |issue| issue.severity == :breaking },
          warning: @issues.count { |issue| issue.severity == :warning },
          note: @issues.count { |issue| issue.severity == :note }
        },
        issues: @issues.map(&:to_h)
      }
    end

    def to_text
      lines = []
      lines << "mode: #{mode}"
      lines << "source schema: #{source_path}"
      lines << "candidate schema: #{candidate_path}"
      lines << "rule: #{rule_text}"
      lines << "compatible: #{compatible?}"
      lines << "breaking issues: #{@issues.count { |issue| issue.severity == :breaking }}"
      lines << "warnings: #{@issues.count { |issue| issue.severity == :warning }}"
      if @issues.empty?
        lines << 'issues: none'
      else
        lines << 'issues:'
        @issues.each do |issue|
          label = issue.severity.to_s.upcase
          line = "- [#{label}] #{issue.path}: #{issue.message}"
          line += " (#{issue.details})" if issue.details && !issue.details.empty?
          lines << line
        end
      end
      lines.join("\n")
    end

    private

    def normalize_path(path)
      return '/' if path.nil? || path.empty?

      path
    end

    def rule_text
      if mode == 'backward'
        'candidate must accept every instance accepted by the source schema'
      else
        'source schema must accept every instance accepted by the candidate schema'
      end
    end
  end

  module Helpers
    module_function

    def deep_copy(value)
      Marshal.load(Marshal.dump(value))
    end

    def deep_stringify(value)
      case value
      when Hash
        value.each_with_object({}) do |(key, inner), result|
          result[key.to_s] = deep_stringify(inner)
        end
      when Array
        value.map { |item| deep_stringify(item) }
      else
        value
      end
    end

    def canonical_json(value)
      JSON.generate(sort_for_json(value))
    end

    def escape_pointer(token)
      token.to_s.gsub('~', '~0').gsub('/', '~1')
    end

    def pointer_join(path, token)
      path = '' if path == '/'
      "#{path}/#{escape_pointer(token)}"
    end

    def value_set(values)
      Array(values).map { |value| canonical_json(value) }.to_set
    end

    def type_set(schema)
      return Set.new if schema == true || schema == false || !schema.is_a?(Hash)

      raw = schema['type']
      types = case raw
              when nil then []
              when Array then raw.map(&:to_s)
              else [raw.to_s]
              end
      types << 'null' if schema['nullable'] == true
      types.to_set
    end

    def schema_accepts_type?(schema, type_name)
      return false if schema == false
      return true if schema == true || !schema.is_a?(Hash)

      types = type_set(schema)
      return true if types.empty?

      case type_name
      when 'integer'
        types.include?('integer') || types.include?('number')
      else
        types.include?(type_name)
      end
    end

    def candidate_covers_type?(candidate_types, source_type)
      return true if candidate_types.empty?

      case source_type
      when 'integer'
        candidate_types.include?('integer') || candidate_types.include?('number')
      else
        candidate_types.include?(source_type)
      end
    end

    def finite_value_schema?(schema)
      schema.is_a?(Hash) && (schema.key?('const') || schema.key?('enum'))
    end

    def finite_values(schema)
      return [schema['const']] if schema.key?('const')
      return Array(schema['enum']) if schema.key?('enum')

      []
    end

    def truthy?(value)
      value == true
    end

    def sort_for_json(value)
      case value
      when Hash
        value.keys.sort.each_with_object({}) do |key, result|
          result[key] = sort_for_json(value[key])
        end
      when Array
        value.map { |item| sort_for_json(item) }
      else
        value
      end
    end

    def parse_yaml(text)
      YAML.safe_load(text, [], [], true)
    rescue ArgumentError
      YAML.safe_load(text, permitted_classes: [], permitted_symbols: [], aliases: true)
    end

    def integer_value?(value)
      value.is_a?(Integer)
    end

    def number_value?(value)
      value.is_a?(Numeric)
    end

    def regex_matches?(pattern, candidate)
      Regexp.new(pattern).match?(candidate)
    rescue RegexpError
      false
    end
  end

  class Loader
    def self.load_file(path)
      text = File.read(path)
      value = parse_by_extension(path, text)
      normalized = Helpers.deep_stringify(value)
      unless normalized.is_a?(Hash) || normalized == true || normalized == false
        raise Error, "Schema root must be an object or boolean: #{path}"
      end

      normalized
    rescue Errno::ENOENT
      raise Error, "Schema file not found: #{path}"
    rescue Psych::SyntaxError => e
      raise Error, "YAML parse error in #{path}: #{e.message}"
    rescue JSON::ParserError => e
      raise Error, "JSON parse error in #{path}: #{e.message}"
    end

    def self.parse_by_extension(path, text)
      extension = File.extname(path).downcase
      if %w[.yaml .yml].include?(extension)
        Helpers.parse_yaml(text)
      else
        JSON.parse(text)
      end
    rescue JSON::ParserError
      Helpers.parse_yaml(text)
    end
  end

  class Resolver
    include Helpers

    def initialize(root_schema)
      @root = Helpers.deep_stringify(root_schema)
      @resolved_cache = {}
    end

    def resolve(schema = @root, stack = [])
      case schema
      when TrueClass, FalseClass
        schema
      when Array
        schema.map { |item| resolve(item, stack) }
      when Hash
        resolve_hash(schema, stack)
      else
        schema
      end
    end

    private

    def resolve_hash(schema, stack)
      current = Helpers.deep_stringify(schema)

      if current.key?('$ref')
        reference = current['$ref']
        raise Error, "External $ref is not supported: #{reference}" unless reference.start_with?('#')
        raise Error, "Cyclic $ref detected: #{(stack + [reference]).join(' -> ')}" if stack.include?(reference)

        resolved = Helpers.deep_copy(resolve_ref(reference, stack + [reference]))
        current = merge_all_of_fragments(resolved, current.reject { |key, _| key == '$ref' }, '/')
      end

      transformed = current.each_with_object({}) do |(key, value), result|
        next if annotation_key?(key)

        result[key] = resolve(value, stack)
      end

      transformed = normalize_nullable(transformed)

      if transformed['allOf'].is_a?(Array)
        base = transformed.reject { |key, _| key == 'allOf' }
        transformed['allOf'].each do |fragment|
          base = merge_all_of_fragments(base, fragment, '/')
        end
        transformed = base
      end

      transformed.delete('$defs')
      transformed.delete('definitions')
      transformed
    end

    def resolve_ref(reference, stack)
      @resolved_cache[reference] ||= resolve(pointer_lookup(reference), stack)
    end

    def pointer_lookup(reference)
      return Helpers.deep_copy(@root) if reference == '#'

      current = @root
      reference.sub(%r{\A#/}, '').split('/').each do |segment|
        key = segment.gsub('~1', '/').gsub('~0', '~')
        current = if current.is_a?(Hash)
                    raise Error, "Unresolved $ref segment #{key.inspect} in #{reference}" unless current.key?(key)

                    current[key]
                  elsif current.is_a?(Array)
                    index = Integer(key)
                    raise Error, "Unresolved $ref index #{key.inspect} in #{reference}" unless index >= 0 && index < current.length

                    current[index]
                  else
                    raise Error, "Invalid $ref target in #{reference}"
                  end
      end

      Helpers.deep_copy(current)
    rescue ArgumentError
      raise Error, "Invalid array index in $ref #{reference}"
    end

    def annotation_key?(key)
      ANNOTATION_KEYS.include?(key) || key.start_with?('x-')
    end

    def normalize_nullable(schema)
      return schema unless schema.is_a?(Hash)
      return schema unless schema['nullable'] == true

      types = Helpers.type_set(schema).to_a
      types = ['null'] if types.empty?
      types << 'null' unless types.include?('null')
      normalized = Helpers.deep_copy(schema)
      normalized.delete('nullable')
      normalized['type'] = types.sort
      normalized
    end

    def merge_all_of_fragments(base, overlay, path)
      return false if base == false || overlay == false
      return Helpers.deep_copy(overlay) if base == true
      return Helpers.deep_copy(base) if overlay == true

      base = Helpers.deep_stringify(base)
      overlay = Helpers.deep_stringify(overlay)
      result = Helpers.deep_copy(base)

      overlay.each do |key, value|
        next if annotation_key?(key)

        if result.key?(key)
          result[key] = merge_keyword(key, result[key], value, Helpers.pointer_join(path, key))
        else
          result[key] = Helpers.deep_copy(value)
        end
      end

      normalize_nullable(result)
    end

    def merge_keyword(key, left, right, path)
      return Helpers.deep_copy(left) if left == right

      case key
      when 'type'
        merge_types(left, right, path)
      when 'required'
        (Array(left) + Array(right)).map(&:to_s).uniq.sort
      when 'enum'
        merge_enums(left, right)
      when 'const'
        raise Error, "Conflicting allOf const values at #{path}" unless left == right

        left
      when 'properties', 'patternProperties', '$defs', 'definitions'
        merge_schema_map(left, right, path)
      when 'additionalProperties', 'propertyNames', 'items', 'additionalItems', 'contains'
        merge_all_of_fragments(left, right, path)
      when 'minimum', 'exclusiveMinimum', 'minLength', 'minItems', 'minProperties', 'minContains'
        [left, right].compact.max
      when 'maximum', 'exclusiveMaximum', 'maxLength', 'maxItems', 'maxProperties', 'maxContains'
        [left, right].compact.min
      when 'uniqueItems'
        Helpers.truthy?(left) || Helpers.truthy?(right)
      when 'multipleOf'
        merge_multiple_of(left, right, path)
      when 'pattern', 'format', 'contentEncoding', 'contentMediaType'
        raise Error, "Unsupported allOf merge for #{key} at #{path}" unless left == right

        left
      else
        raise Error, "Unsupported allOf merge for #{key} at #{path}" unless left == right

        left
      end
    end

    def merge_types(left, right, path)
      left_set = Array(left).map(&:to_s).to_set
      right_set = Array(right).map(&:to_s).to_set
      merged = if left_set.empty?
                 right_set
               elsif right_set.empty?
                 left_set
               else
                 left_set & right_set
               end
      raise Error, "allOf type intersection is empty at #{path}" if merged.empty?

      merged.to_a.sort
    end

    def merge_enums(left, right)
      left_values = Helpers.value_set(left)
      right_values = Helpers.value_set(right)
      allowed = left_values & right_values
      raise Error, 'allOf enum intersection is empty' if allowed.empty?

      Array(left).select { |value| allowed.include?(Helpers.canonical_json(value)) }
    end

    def merge_schema_map(left, right, path)
      left = Helpers.deep_stringify(left || {})
      right = Helpers.deep_stringify(right || {})
      keys = (left.keys + right.keys).uniq
      keys.each_with_object({}) do |key, result|
        if left.key?(key) && right.key?(key)
          result[key] = merge_all_of_fragments(left[key], right[key], Helpers.pointer_join(path, key))
        elsif left.key?(key)
          result[key] = Helpers.deep_copy(left[key])
        else
          result[key] = Helpers.deep_copy(right[key])
        end
      end
    end

    def merge_multiple_of(left, right, path)
      left_r = Rational(left.to_s)
      right_r = Rational(right.to_s)
      larger = [left_r, right_r].max
      smaller = [left_r, right_r].min
      if (larger / smaller).denominator == 1
        larger.to_f % 1 == 0 ? larger.to_i : larger.to_f
      else
        raise Error, "Unsupported allOf multipleOf merge at #{path}"
      end
    rescue StandardError
      raise Error, "Unsupported allOf multipleOf merge at #{path}"
    end
  end

  class Validator
    include Helpers

    def initialize(strict_format: false)
      @strict_format = strict_format
    end

    def valid?(value, schema)
      return true if schema == true
      return false if schema == false
      return false unless schema.is_a?(Hash)

      schema = Helpers.deep_stringify(schema)
      return false unless validate_boolean_combiners(value, schema)
      return false unless validate_type(value, schema)
      return false unless validate_const_and_enum(value, schema)
      return false unless validate_numeric(value, schema)
      return false unless validate_string(value, schema)
      return false unless validate_object(value, schema)
      return false unless validate_array(value, schema)
      return false unless validate_format(value, schema)

      true
    end

    private

    def validate_boolean_combiners(value, schema)
      if schema['allOf'].is_a?(Array)
        return false unless schema['allOf'].all? { |fragment| valid?(value, fragment) }
      end
      if schema['anyOf'].is_a?(Array)
        return false unless schema['anyOf'].any? { |fragment| valid?(value, fragment) }
      end
      if schema['oneOf'].is_a?(Array)
        return false unless schema['oneOf'].count { |fragment| valid?(value, fragment) } == 1
      end
      if schema.key?('not')
        return false if valid?(value, schema['not'])
      end
      return true unless schema.key?('if')

      if valid?(value, schema['if'])
        return false if schema.key?('then') && !valid?(value, schema['then'])
      elsif schema.key?('else')
        return false unless valid?(value, schema['else'])
      end

      true
    end

    def validate_type(value, schema)
      types = Helpers.type_set(schema)
      return true if types.empty?

      types.any? { |type_name| type_matches?(value, type_name) }
    end

    def type_matches?(value, type_name)
      case type_name
      when 'null' then value.nil?
      when 'boolean' then value == true || value == false
      when 'string' then value.is_a?(String)
      when 'integer' then Helpers.integer_value?(value)
      when 'number' then Helpers.number_value?(value)
      when 'object' then value.is_a?(Hash)
      when 'array' then value.is_a?(Array)
      else false
      end
    end

    def validate_const_and_enum(value, schema)
      return false if schema.key?('const') && value != schema['const']
      return true unless schema.key?('enum')

      schema['enum'].any? { |candidate| candidate == value }
    end

    def validate_numeric(value, schema)
      return true unless Helpers.number_value?(value)

      if schema.key?('minimum') && value < schema['minimum']
        return false
      end
      if schema.key?('exclusiveMinimum') && value <= schema['exclusiveMinimum']
        return false
      end
      if schema.key?('maximum') && value > schema['maximum']
        return false
      end
      if schema.key?('exclusiveMaximum') && value >= schema['exclusiveMaximum']
        return false
      end
      if schema.key?('multipleOf')
        dividend = Rational(value.to_s) / Rational(schema['multipleOf'].to_s)
        return false unless dividend.denominator == 1
      end

      true
    rescue StandardError
      false
    end

    def validate_string(value, schema)
      return true unless value.is_a?(String)

      return false if schema.key?('minLength') && value.length < schema['minLength']
      return false if schema.key?('maxLength') && value.length > schema['maxLength']
      return false if schema.key?('pattern') && !Helpers.regex_matches?(schema['pattern'], value)

      true
    end

    def validate_object(value, schema)
      return true unless value.is_a?(Hash)

      required = Array(schema['required']).map(&:to_s)
      return false unless required.all? { |name| value.key?(name) }
      return false if schema.key?('minProperties') && value.length < schema['minProperties']
      return false if schema.key?('maxProperties') && value.length > schema['maxProperties']

      if schema.key?('propertyNames')
        return false unless value.keys.all? { |name| valid?(name, schema['propertyNames']) }
      end

      dependent_required = Helpers.deep_stringify(schema['dependentRequired'] || {})
      dependent_required.each do |name, dependencies|
        next unless value.key?(name)

        return false unless Array(dependencies).all? { |dependency| value.key?(dependency.to_s) }
      end

      properties = Helpers.deep_stringify(schema['properties'] || {})
      pattern_properties = Helpers.deep_stringify(schema['patternProperties'] || {})
      additional = schema.key?('additionalProperties') ? schema['additionalProperties'] : true

      value.each do |name, inner|
        matched = false
        if properties.key?(name)
          return false unless valid?(inner, properties[name])
          matched = true
        end

        pattern_properties.each do |pattern, property_schema|
          next unless Helpers.regex_matches?(pattern, name)

          return false unless valid?(inner, property_schema)
          matched = true
        end

        next if matched
        return false unless valid_additional?(inner, additional)
      end

      true
    end

    def valid_additional?(value, schema)
      case schema
      when true, nil then true
      when false then false
      else valid?(value, schema)
      end
    end

    def validate_array(value, schema)
      return true unless value.is_a?(Array)

      return false if schema.key?('minItems') && value.length < schema['minItems']
      return false if schema.key?('maxItems') && value.length > schema['maxItems']
      if Helpers.truthy?(schema['uniqueItems'])
        canonical = value.map { |item| Helpers.canonical_json(item) }
        return false unless canonical.uniq.length == canonical.length
      end

      prefix_items = tuple_items(schema)
      tail_schema = tail_items_schema(schema)

      value.each_with_index do |item, index|
        if index < prefix_items.length
          return false unless valid?(item, prefix_items[index])
        else
          return false unless valid_additional?(item, tail_schema)
        end
      end

      if schema.key?('contains')
        matches = value.count { |item| valid?(item, schema['contains']) }
        min_contains = schema.key?('minContains') ? schema['minContains'] : 1
        max_contains = schema['maxContains']
        return false if matches < min_contains
        return false if max_contains && matches > max_contains
      end

      true
    end

    def tuple_items(schema)
      raw = if schema['prefixItems'].is_a?(Array)
              schema['prefixItems']
            elsif schema['items'].is_a?(Array)
              schema['items']
            else
              []
            end
      raw.map { |item| Helpers.deep_stringify(item) }
    end

    def tail_items_schema(schema)
      if schema['prefixItems'].is_a?(Array)
        return schema.key?('items') ? schema['items'] : true
      end
      if schema['items'].is_a?(Array)
        return schema.key?('additionalItems') ? schema['additionalItems'] : true
      end

      schema.key?('items') ? schema['items'] : true
    end

    def validate_format(value, schema)
      return true unless @strict_format
      return true unless value.is_a?(String)
      return true unless schema.key?('format')

      case schema['format']
      when 'date-time'
        value.match?(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\z/)
      when 'date'
        value.match?(/\A\d{4}-\d{2}-\d{2}\z/)
      when 'uuid'
        value.match?(/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/)
      when 'email'
        value.match?(/\A[^@\s]+@[^@\s]+\.[^@\s]+\z/)
      when 'uri', 'url'
        value.match?(/\Ahttps?:\/\//)
      else
        true
      end
    end
  end

  class Comparator
    include Helpers

    def initialize(strict_format: false)
      @strict_format = strict_format
      @validator = Validator.new(strict_format: strict_format)
    end

    def compare(source_schema, candidate_schema, report)
      compare_schema(source_schema, candidate_schema, '', report)
    end

    private

    def compare_schema(source, candidate, path, report)
      return if source == candidate
      return if source == false

      if candidate == true
        compare_unsupported_keyword_changes(source, candidate, path, report)
        return
      end

      if source == true
        report.breaking(path, 'Candidate adds constraints to a previously unconstrained schema')
        compare_unsupported_keyword_changes(source, candidate, path, report)
        return
      end

      if candidate == false
        report.breaking(path, 'Candidate rejects values previously accepted by the source schema')
        return
      end

      source = Helpers.deep_stringify(source)
      candidate = Helpers.deep_stringify(candidate)

      compare_unsupported_keyword_changes(source, candidate, path, report)

      if Helpers.finite_value_schema?(source)
        compare_finite_schema(source, candidate, path, report)
        return
      end

      compare_enum_and_const_constraints(source, candidate, path, report)
      compare_type_coverage(source, candidate, path, report)
      compare_numeric_constraints(source, candidate, path, report) if source_may_accept_numeric?(source)
      compare_string_constraints(source, candidate, path, report) if Helpers.schema_accepts_type?(source, 'string')
      compare_object_constraints(source, candidate, path, report) if Helpers.schema_accepts_type?(source, 'object')
      compare_array_constraints(source, candidate, path, report) if Helpers.schema_accepts_type?(source, 'array')
    end

    def compare_finite_schema(source, candidate, path, report)
      failures = Helpers.finite_values(source).reject { |value| @validator.valid?(value, candidate) }
      return if failures.empty?

      sample = failures.first(3).map { |value| Helpers.canonical_json(value) }.join(', ')
      details = failures.length > 3 ? "Examples rejected by candidate: #{sample} and #{failures.length - 3} more" : "Examples rejected by candidate: #{sample}"
      report.breaking(path, 'Candidate rejects values enumerated by the source schema', details)
    end

    def compare_enum_and_const_constraints(source, candidate, path, report)
      if !source.key?('const') && candidate.key?('const')
        report.breaking(path, 'Candidate narrows the schema to a single constant')
      end
      if !source.key?('enum') && candidate.key?('enum')
        report.breaking(path, 'Candidate adds an enum restriction')
      end
    end

    def compare_type_coverage(source, candidate, path, report)
      source_types = Helpers.type_set(source)
      candidate_types = Helpers.type_set(candidate)

      if source_types.empty?
        unless candidate_types.empty?
          report.breaking(path, "Candidate adds an explicit type restriction: #{candidate_types.to_a.sort.join(', ')}")
        end
        return
      end

      removed = source_types.reject { |type_name| Helpers.candidate_covers_type?(candidate_types, type_name) }
      return if removed.empty?

      report.breaking(path, "Candidate no longer accepts source types: #{removed.to_a.sort.join(', ')}")
    end

    def source_may_accept_numeric?(schema)
      Helpers.schema_accepts_type?(schema, 'number') || Helpers.schema_accepts_type?(schema, 'integer')
    end

    def compare_numeric_constraints(source, candidate, path, report)
      compare_lower_bound(source, candidate, path, report)
      compare_upper_bound(source, candidate, path, report)
      compare_multiple_of(source, candidate, path, report)
    end

    def compare_lower_bound(source, candidate, path, report)
      source_lower = lower_bound(source)
      candidate_lower = lower_bound(candidate)
      return if candidate_lower.nil?
      if source_lower.nil?
        report.breaking(path, 'Candidate adds a numeric lower bound')
        return
      end
      if candidate_lower[:value] > source_lower[:value]
        report.breaking(path, 'Candidate raises the numeric lower bound')
      elsif candidate_lower[:value] == source_lower[:value] && candidate_lower[:exclusive] && !source_lower[:exclusive]
        report.breaking(path, 'Candidate makes the numeric lower bound exclusive')
      end
    end

    def compare_upper_bound(source, candidate, path, report)
      source_upper = upper_bound(source)
      candidate_upper = upper_bound(candidate)
      return if candidate_upper.nil?
      if source_upper.nil?
        report.breaking(path, 'Candidate adds a numeric upper bound')
        return
      end
      if candidate_upper[:value] < source_upper[:value]
        report.breaking(path, 'Candidate lowers the numeric upper bound')
      elsif candidate_upper[:value] == source_upper[:value] && candidate_upper[:exclusive] && !source_upper[:exclusive]
        report.breaking(path, 'Candidate makes the numeric upper bound exclusive')
      end
    end

    def compare_multiple_of(source, candidate, path, report)
      return unless candidate.key?('multipleOf')
      unless source.key?('multipleOf')
        report.breaking(path, 'Candidate adds a multipleOf constraint')
        return
      end

      source_value = Rational(source['multipleOf'].to_s)
      candidate_value = Rational(candidate['multipleOf'].to_s)
      ratio = source_value / candidate_value
      return if ratio.denominator == 1

      report.breaking(path, 'Candidate makes the multipleOf constraint stricter')
    rescue StandardError
      report.warning(path, 'Could not reason about multipleOf compatibility exactly')
    end

    def compare_string_constraints(source, candidate, path, report)
      compare_minimum_like(source, candidate, path, report, 'minLength', 0, 'Candidate increases minLength')
      compare_maximum_like(source, candidate, path, report, 'maxLength', nil, 'Candidate decreases maxLength')

      if candidate.key?('pattern')
        if !source.key?('pattern')
          report.breaking(path, 'Candidate adds a string pattern restriction')
        elsif source['pattern'] != candidate['pattern']
          report.warning(path, 'Pattern changed and may be stricter than the source pattern')
        end
      end

      if @strict_format && candidate.key?('format')
        if !source.key?('format')
          report.breaking(path, 'Candidate adds a strict string format requirement')
        elsif source['format'] != candidate['format']
          report.warning(path, 'String format changed and may need manual review')
        end
      end

      %w[contentEncoding contentMediaType].each do |key|
        next unless candidate.key?(key)
        next if source[key] == candidate[key]

        if source.key?(key)
          report.warning(path, "#{key} changed and may affect validator behavior")
        else
          report.warning(path, "Candidate adds #{key}; some validators treat that as a restriction")
        end
      end
    end

    def compare_object_constraints(source, candidate, path, report)
      compare_minimum_like(source, candidate, path, report, 'minProperties', 0, 'Candidate increases minProperties')
      compare_maximum_like(source, candidate, path, report, 'maxProperties', nil, 'Candidate decreases maxProperties')
      compare_required(source, candidate, path, report)
      compare_property_names(source, candidate, path, report)
      compare_dependent_required(source, candidate, path, report)
      compare_properties(source, candidate, path, report)
      compare_pattern_properties(source, candidate, path, report)
      compare_additional_properties(source, candidate, path, report)
    end

    def compare_required(source, candidate, path, report)
      source_required = Array(source['required']).map(&:to_s).to_set
      candidate_required = Array(candidate['required']).map(&:to_s).to_set
      added = candidate_required - source_required
      return if added.empty?

      report.breaking(path, "Candidate adds required properties: #{added.to_a.sort.join(', ')}")
    end

    def compare_property_names(source, candidate, path, report)
      return unless candidate.key?('propertyNames')
      unless source.key?('propertyNames')
        report.breaking(Helpers.pointer_join(path, 'propertyNames'), 'Candidate adds a propertyNames restriction')
        return
      end

      compare_schema(source['propertyNames'], candidate['propertyNames'], Helpers.pointer_join(path, 'propertyNames'), report)
    end

    def compare_dependent_required(source, candidate, path, report)
      source_map = Helpers.deep_stringify(source['dependentRequired'] || {})
      candidate_map = Helpers.deep_stringify(candidate['dependentRequired'] || {})

      candidate_map.each do |name, dependencies|
        source_dependencies = Array(source_map[name]).map(&:to_s).to_set
        candidate_dependencies = Array(dependencies).map(&:to_s).to_set
        added = candidate_dependencies - source_dependencies
        next if added.empty?

        report.breaking(Helpers.pointer_join(path, 'dependentRequired'), "Candidate adds dependentRequired entries for #{name}: #{added.to_a.sort.join(', ')}")
      end
    end

    def compare_properties(source, candidate, path, report)
      source_properties = Helpers.deep_stringify(source['properties'] || {})
      candidate_properties = Helpers.deep_stringify(candidate['properties'] || {})
      fallback = candidate_additional_properties(candidate)

      source_properties.each do |name, property_schema|
        property_path = Helpers.pointer_join(Helpers.pointer_join(path, 'properties'), name)
        if candidate_properties.key?(name)
          compare_schema(property_schema, candidate_properties[name], property_path, report)
          next
        end

        case fallback
        when false
          report.breaking(property_path, "Candidate removes property #{name} while rejecting unmatched properties")
        when true
          report.note(property_path, "Candidate removes explicit schema for #{name} but still allows the property via additionalProperties")
        else
          compare_schema(property_schema, fallback, property_path, report)
        end
      end
    end

    def compare_pattern_properties(source, candidate, path, report)
      source_patterns = Helpers.deep_stringify(source['patternProperties'] || {})
      candidate_patterns = Helpers.deep_stringify(candidate['patternProperties'] || {})
      fallback = candidate_additional_properties(candidate)

      source_patterns.each do |pattern, property_schema|
        pattern_path = Helpers.pointer_join(Helpers.pointer_join(path, 'patternProperties'), pattern)
        if candidate_patterns.key?(pattern)
          compare_schema(property_schema, candidate_patterns[pattern], pattern_path, report)
          next
        end

        case fallback
        when false
          report.breaking(pattern_path, "Candidate removes patternProperties #{pattern} while rejecting unmatched properties")
        when true
          report.note(pattern_path, "Candidate removes explicit patternProperties #{pattern} but still allows unmatched keys")
        else
          compare_schema(property_schema, fallback, pattern_path, report)
        end
      end

      added_patterns = candidate_patterns.keys - source_patterns.keys
      unless added_patterns.empty?
        report.warning(Helpers.pointer_join(path, 'patternProperties'), 'Candidate adds new patternProperties rules that may reject keys previously allowed', added_patterns.sort.join(', '))
      end
    end

    def compare_additional_properties(source, candidate, path, report)
      source_additional = candidate_additional_properties(source)
      candidate_additional = candidate_additional_properties(candidate)
      additional_path = Helpers.pointer_join(path, 'additionalProperties')

      if source_additional == true
        if candidate_additional == false || candidate_additional.is_a?(Hash)
          report.breaking(additional_path, 'Candidate restricts additionalProperties that were previously allowed')
        end
        return
      end

      return if source_additional == false

      case candidate_additional
      when true
        nil
      when false
        report.breaking(additional_path, 'Candidate disallows additionalProperties previously permitted by the source schema')
      else
        compare_schema(source_additional, candidate_additional, additional_path, report)
      end
    end

    def compare_array_constraints(source, candidate, path, report)
      compare_minimum_like(source, candidate, path, report, 'minItems', 0, 'Candidate increases minItems')
      compare_maximum_like(source, candidate, path, report, 'maxItems', nil, 'Candidate decreases maxItems')
      if Helpers.truthy?(candidate['uniqueItems']) && !Helpers.truthy?(source['uniqueItems'])
        report.breaking(path, 'Candidate adds uniqueItems')
      end
      compare_prefix_items(source, candidate, path, report)
      compare_tail_items(source, candidate, path, report)
      compare_contains(source, candidate, path, report)
    end

    def compare_prefix_items(source, candidate, path, report)
      source_prefix = tuple_items(source)
      candidate_prefix = tuple_items(candidate)
      source_prefix.each_with_index do |schema, index|
        item_path = Helpers.pointer_join(Helpers.pointer_join(path, 'prefixItems'), index)
        if index < candidate_prefix.length
          compare_schema(schema, candidate_prefix[index], item_path, report)
        else
          fallback = tail_items_schema(candidate)
          case fallback
          when false
            report.breaking(item_path, 'Candidate no longer allows tuple items at this position')
          when true
            nil
          else
            compare_schema(schema, fallback, item_path, report)
          end
        end
      end
    end

    def compare_tail_items(source, candidate, path, report)
      source_tail = tail_items_schema(source)
      candidate_tail = tail_items_schema(candidate)
      items_path = Helpers.pointer_join(path, 'items')

      if source_tail == true
        if candidate_tail == false || candidate_tail.is_a?(Hash)
          report.breaking(items_path, 'Candidate constrains array items that were previously unconstrained')
        end
        return
      end

      return if source_tail == false

      case candidate_tail
      when true
        nil
      when false
        report.breaking(items_path, 'Candidate rejects array items previously accepted by the source schema')
      else
        compare_schema(source_tail, candidate_tail, items_path, report)
      end
    end

    def compare_contains(source, candidate, path, report)
      contains_path = Helpers.pointer_join(path, 'contains')
      if candidate.key?('contains')
        unless source.key?('contains')
          report.breaking(contains_path, 'Candidate adds a contains requirement')
          return
        end
        compare_schema(source['contains'], candidate['contains'], contains_path, report)
      end
      compare_minimum_like(source, candidate, contains_path, report, 'minContains', 1, 'Candidate increases minContains') if candidate.key?('contains')
      compare_maximum_like(source, candidate, contains_path, report, 'maxContains', nil, 'Candidate decreases maxContains') if candidate.key?('maxContains')
    end

    def compare_unsupported_keyword_changes(source, candidate, path, report)
      UNSUPPORTED_COMPARISON_KEYWORDS.each do |key|
        next if source[key] == candidate[key]
        next unless source.key?(key) || candidate.key?(key)

        report.warning(Helpers.pointer_join(path, key), "Keyword #{key} changed; manual review is recommended because exact compatibility is undecidable here")
      end
    end

    def compare_minimum_like(source, candidate, path, report, key, default_source, message)
      return unless candidate.key?(key)

      source_value = source.key?(key) ? source[key] : default_source
      if source_value.nil?
        report.breaking(Helpers.pointer_join(path, key), message)
        return
      end
      report.breaking(Helpers.pointer_join(path, key), message) if candidate[key] > source_value
    end

    def compare_maximum_like(source, candidate, path, report, key, default_source, message)
      return unless candidate.key?(key)

      source_value = source.key?(key) ? source[key] : default_source
      if source_value.nil?
        report.breaking(Helpers.pointer_join(path, key), message)
        return
      end
      report.breaking(Helpers.pointer_join(path, key), message) if candidate[key] < source_value
    end

    def lower_bound(schema)
      return { value: schema['exclusiveMinimum'], exclusive: true } if schema.key?('exclusiveMinimum')
      return { value: schema['minimum'], exclusive: false } if schema.key?('minimum')

      nil
    end

    def upper_bound(schema)
      return { value: schema['exclusiveMaximum'], exclusive: true } if schema.key?('exclusiveMaximum')
      return { value: schema['maximum'], exclusive: false } if schema.key?('maximum')

      nil
    end

    def candidate_additional_properties(schema)
      schema.key?('additionalProperties') ? schema['additionalProperties'] : true
    end

    def tuple_items(schema)
      raw = if schema['prefixItems'].is_a?(Array)
              schema['prefixItems']
            elsif schema['items'].is_a?(Array)
              schema['items']
            else
              []
            end
      raw.map { |item| Helpers.deep_stringify(item) }
    end

    def tail_items_schema(schema)
      if schema['prefixItems'].is_a?(Array)
        return schema.key?('items') ? schema['items'] : true
      end
      if schema['items'].is_a?(Array)
        return schema.key?('additionalItems') ? schema['additionalItems'] : true
      end

      schema.key?('items') ? schema['items'] : true
    end
  end

  class CLI
    def initialize(argv)
      @argv = argv.dup
    end

    def run
      options = {
        format: 'text',
        mode: 'backward',
        strict_format: false,
        fail_on: 'breaking'
      }

      parser = OptionParser.new do |opts|
        opts.banner = 'Usage: JsonSchemaCompatibilityGuard.rb [options] OLD_SCHEMA NEW_SCHEMA'
        opts.separator ''
        opts.separator 'Checks compatibility for JSON Schema, OpenAPI fragments, MCP tool schemas,'
        opts.separator 'LLM structured output contracts, and event payload definitions.'
        opts.separator ''
        opts.on('--format FORMAT', 'Output format: text or json') do |value|
          options[:format] = value
        end
        opts.on('--mode MODE', 'Compatibility mode: backward or forward') do |value|
          options[:mode] = value
        end
        opts.on('--strict-format', 'Treat common string formats as validated constraints') do
          options[:strict_format] = true
        end
        opts.on('--fail-on LEVEL', 'Exit non-zero on: breaking or warning') do |value|
          options[:fail_on] = value
        end
        opts.on('-h', '--help', 'Show this help') do
          puts opts
          return COMPATIBLE_EXIT
        end
      end

      parser.parse!(@argv)
      validate_options!(options)
      raise Error, parser.banner if @argv.length != 2

      old_path, new_path = @argv
      old_schema = Resolver.new(Loader.load_file(old_path)).resolve
      new_schema = Resolver.new(Loader.load_file(new_path)).resolve

      source_schema, candidate_schema, source_path, candidate_path = if options[:mode] == 'backward'
                                                                       [old_schema, new_schema, old_path, new_path]
                                                                     else
                                                                       [new_schema, old_schema, new_path, old_path]
                                                                     end

      report = Report.new(mode: options[:mode], source_path: source_path, candidate_path: candidate_path)
      Comparator.new(strict_format: options[:strict_format]).compare(source_schema, candidate_schema, report)

      output = options[:format] == 'json' ? JSON.pretty_generate(report.to_h) : report.to_text
      puts output
      report.exit_code(options[:fail_on])
    rescue Error => e
      warn "error: #{e.message}"
      ERROR_EXIT
    end

    private

    def validate_options!(options)
      unless %w[text json].include?(options[:format])
        raise Error, "Unsupported format: #{options[:format]}"
      end
      unless %w[backward forward].include?(options[:mode])
        raise Error, "Unsupported mode: #{options[:mode]}"
      end
      unless %w[breaking warning].include?(options[:fail_on])
        raise Error, "Unsupported fail-on level: #{options[:fail_on]}"
      end
    end
  end
end

if $PROGRAM_NAME == __FILE__
  exit JsonSchemaCompatibilityGuard::CLI.new(ARGV).run
end

=begin
This solves JSON Schema compatibility checking for MCP tool schemas, OpenAI structured outputs, Anthropic tool definitions, OpenAPI request and response contracts, and event payload validation in CI. Built because schema drift is one of those boring failures that burns release time: one extra required field, one narrower enum, one stricter array rule, and suddenly agents, backends, or pipelines start rejecting traffic that used to work.

Use it when you need a fast Ruby script to gate pull requests, compare old and new schema files, or protect Rails, Sidekiq, serverless, and data pipeline contracts from accidental breaking changes. The trick: it resolves local refs, folds common allOf patterns, handles object, array, string, and numeric compatibility rules, and gives warnings when the schema uses constructs that need human review instead of pretending the answer is obvious.

Drop this into a repo as JsonSchemaCompatibilityGuard.rb, wire it into GitHub Actions, CI, or a release script, and run it before publishing MCP servers, LLM response schemas, API versions, or event producers. If someone searches for Ruby JSON Schema compatibility checker, MCP schema breaking changes, structured output schema diff, OpenAPI contract guard, or event schema CI gate, this is exactly the kind of standalone file they can fork and use.
=end
