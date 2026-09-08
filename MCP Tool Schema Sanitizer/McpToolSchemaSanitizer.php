<?php

declare(strict_types=1);

final class McpToolSchemaSanitizationException extends InvalidArgumentException
{
}

final class McpToolSchemaSanitizationResult implements JsonSerializable
{
    public readonly array $schema;
    public readonly string $fingerprint;
    public readonly array $warnings;
    public readonly array $errors;
    public readonly array $metrics;

    public function __construct(
        array $schema,
        string $fingerprint,
        array $warnings,
        array $errors,
        array $metrics
    ) {
        $this->schema = $schema;
        $this->fingerprint = $fingerprint;
        $this->warnings = array_values($warnings);
        $this->errors = array_values($errors);
        $this->metrics = $metrics;
    }

    public function isSafe(): bool
    {
        return $this->errors === [];
    }

    public function assertSafe(): void
    {
        if ($this->isSafe()) {
            return;
        }

        throw new McpToolSchemaSanitizationException(
            "Schema is not safe for production tool use:\n- " . implode("\n- ", $this->errors)
        );
    }

    public function jsonSerialize(): array
    {
        return [
            'schema' => $this->schema,
            'fingerprint' => $this->fingerprint,
            'warnings' => $this->warnings,
            'errors' => $this->errors,
            'metrics' => $this->metrics,
            'safe' => $this->isSafe(),
        ];
    }
}

final class McpToolSchemaSanitizer
{
    private const DEFAULT_OPTIONS = [
        'target' => 'cross_provider',
        'force_closed_objects' => true,
        'inline_local_refs' => true,
        'strip_titles' => true,
        'strip_defaults' => true,
        'strip_examples' => true,
        'convert_nullable_to_optional' => true,
        'sort_keys' => true,
        'max_depth' => 8,
        'max_properties' => 256,
        'max_enum_values' => 256,
    ];

    private const VALID_TARGETS = [
        'cross_provider',
        'openai',
        'anthropic',
        'gemini',
        'generic',
    ];

    private const ALWAYS_STRIP = [
        '$schema',
        '$id',
        '$anchor',
        '$dynamicRef',
        '$dynamicAnchor',
        '$vocabulary',
        '$comment',
        'definitions',
        '$defs',
        'example',
        'examples',
        'readOnly',
        'writeOnly',
        'deprecated',
        'unevaluatedItems',
        'unevaluatedProperties',
        'patternProperties',
        'propertyNames',
        'dependentRequired',
        'dependentSchemas',
        'contentEncoding',
        'contentMediaType',
        'contentSchema',
        'minContains',
        'maxContains',
        'prefixItems',
    ];

    private const KEY_ORDER = [
        'type',
        'description',
        'properties',
        'required',
        'additionalProperties',
        'items',
        'enum',
        'const',
        'format',
        'pattern',
        'minLength',
        'maxLength',
        'minimum',
        'maximum',
        'exclusiveMinimum',
        'exclusiveMaximum',
        'multipleOf',
        'minItems',
        'maxItems',
        'default',
    ];

    private array $options;
    private array $warnings = [];
    private array $errors = [];
    private array $metrics = [
        'nodeCount' => 0,
        'maxDepth' => 0,
        'propertyCount' => 0,
        'enumValueCount' => 0,
        'descriptionBytes' => 0,
        'schemaBytes' => 0,
    ];

    private function __construct(array $options)
    {
        $this->options = array_replace(self::DEFAULT_OPTIONS, $options);
        $this->validateOptions();
    }

    public static function sanitize(array $schema, array $options = []): McpToolSchemaSanitizationResult
    {
        $instance = new self($options);

        return $instance->run($schema);
    }

    public static function sanitizeTool(string $name, array $inputSchema, array $options = []): array
    {
        $result = self::sanitize($inputSchema, $options);

        return [
            'name' => $name,
            'inputSchema' => $result->schema,
            'schemaFingerprint' => $result->fingerprint,
            'schemaWarnings' => $result->warnings,
            'schemaErrors' => $result->errors,
            'schemaMetrics' => $result->metrics,
        ];
    }

    private function run(array $schema): McpToolSchemaSanitizationResult
    {
        if (!$this->isAssoc($schema)) {
            throw new McpToolSchemaSanitizationException(
                'Root schema must be a JSON object represented as an associative PHP array.'
            );
        }

        $sanitized = $this->sanitizeNode($schema, $schema, '$', [], false, 1);
        $sanitized = $this->finalizeRoot($sanitized);

        if ($this->options['sort_keys']) {
            $sanitized = $this->canonicalizeNode($sanitized);
        }

        $canonicalJson = $this->canonicalJson($sanitized);
        $this->metrics['schemaBytes'] = strlen($canonicalJson);
        $this->enforceLimits();

        return new McpToolSchemaSanitizationResult(
            $sanitized,
            hash('sha256', $canonicalJson),
            $this->warnings,
            $this->errors,
            $this->metrics
        );
    }

    private function finalizeRoot(array $schema): array
    {
        if (($schema['type'] ?? null) !== 'object') {
            $this->error(
                '$',
                'Root schema must resolve to an object for MCP tool input. Wrapping primitives in object fields is more portable.'
            );
            $schema = [
                'type' => 'object',
                'properties' => [
                    'value' => $schema,
                ],
                'required' => ['value'],
                'additionalProperties' => false,
                'description' => 'Wrapped primitive schema. Replace with explicit object fields for production use.',
            ];
        }

        if (!isset($schema['properties']) || !is_array($schema['properties'])) {
            $schema['properties'] = [];
        }

        if ($this->options['force_closed_objects']) {
            $schema['additionalProperties'] = false;
        }

        return $schema;
    }

    private function sanitizeNode(
        array $node,
        array $document,
        string $path,
        array $refStack,
        bool $requiredProperty,
        int $depth
    ): array {
        $this->metrics['nodeCount']++;
        $this->metrics['maxDepth'] = max($this->metrics['maxDepth'], $depth);

        $node = $this->inlineLocalRef($node, $document, $path, $refStack);
        $node = $this->collapseCombinators($node, $document, $path, $refStack, $requiredProperty, $depth);
        $node = $this->normalizeCommonKeywords($node, $path);
        $node = $this->normalizeTypeMetadata($node, $path, $requiredProperty);
        $type = $node['type'] ?? null;

        if ($type === 'object') {
            $node = $this->sanitizeObjectNode($node, $document, $path, $refStack, $depth);
        } elseif ($type === 'array') {
            $node = $this->sanitizeArrayNode($node, $document, $path, $refStack, $depth);
        } elseif (is_string($type)) {
            $node = $this->sanitizeScalarNode($node, $path);
        } else {
            $this->error($path, 'Schema node has no concrete type after normalization. Falling back to string.');
            $node['type'] = 'string';
            $node = $this->sanitizeScalarNode($node, $path);
        }

        return $node;
    }

    private function inlineLocalRef(array $node, array $document, string $path, array $refStack): array
    {
        if (!isset($node['$ref'])) {
            return $node;
        }

        if (!is_string($node['$ref'])) {
            $this->error($path, 'The $ref value must be a string. Removing it.');
            unset($node['$ref']);

            return $node;
        }

        if (!$this->options['inline_local_refs']) {
            $this->error($path, 'Local refs are disabled in options, but remote providers usually reject raw $ref pointers.');

            return $node;
        }

        $reference = $node['$ref'];

        if (!str_starts_with($reference, '#/')) {
            $this->error($path, "Only local refs are supported. Rejecting remote ref {$reference}.");
            unset($node['$ref']);

            return $node;
        }

        if (in_array($reference, $refStack, true)) {
            $this->error($path, "Recursive ref {$reference} detected. Recursive schemas are not portable across tool providers.");
            unset($node['$ref']);

            return $node;
        }

        $resolved = $this->resolveLocalRef($document, $reference, $path);
        $overrides = $node;
        unset($overrides['$ref']);

        return $this->mergeSchemas(
            $resolved,
            $overrides,
            $path,
            array_merge($refStack, [$reference])
        );
    }

    private function resolveLocalRef(array $document, string $reference, string $path): array
    {
        $segments = explode('/', substr($reference, 2));
        $cursor = $document;

        foreach ($segments as $segment) {
            $segment = str_replace(['~1', '~0'], ['/', '~'], $segment);

            if (!is_array($cursor) || !array_key_exists($segment, $cursor)) {
                $this->error($path, "Unresolvable ref {$reference}.");

                return ['type' => 'string'];
            }

            $cursor = $cursor[$segment];
        }

        if (!is_array($cursor)) {
            $this->error($path, "Ref {$reference} does not point to an object schema.");

            return ['type' => 'string'];
        }

        return $cursor;
    }

    private function collapseCombinators(
        array $node,
        array $document,
        string $path,
        array $refStack,
        bool $requiredProperty,
        int $depth
    ): array {
        if (isset($node['allOf'])) {
            if (!is_array($node['allOf']) || !array_is_list($node['allOf'])) {
                $this->error($path, 'allOf must be a list. Removing it.');
                unset($node['allOf']);
            } else {
                $base = $node;
                unset($base['allOf']);

                foreach ($node['allOf'] as $index => $fragment) {
                    if (!is_array($fragment)) {
                        $this->error("{$path}/allOf/{$index}", 'Each allOf entry must be an object schema.');
                        continue;
                    }

                    $fragment = $this->inlineLocalRef(
                        $fragment,
                        $document,
                        "{$path}/allOf/{$index}",
                        $refStack
                    );

                    $base = $this->mergeSchemas(
                        $base,
                        $fragment,
                        "{$path}/allOf/{$index}",
                        $refStack
                    );
                }

                $node = $base;
            }
        }

        foreach (['anyOf', 'oneOf'] as $keyword) {
            if (!isset($node[$keyword])) {
                continue;
            }

            if (!is_array($node[$keyword]) || !array_is_list($node[$keyword])) {
                $this->error($path, "{$keyword} must be a list. Removing it.");
                unset($node[$keyword]);
                continue;
            }

            $collapsed = $this->collapseNullableUnion(
                $node[$keyword],
                $document,
                "{$path}/{$keyword}",
                $refStack,
                $requiredProperty,
                $depth
            );

            if ($collapsed === null) {
                $this->error(
                    $path,
                    "{$keyword} is too ambiguous for cross-provider tool schemas. Prefer a single concrete type per field."
                );
                unset($node[$keyword]);
                continue;
            }

            unset($node[$keyword]);
            $node = $this->mergeSchemas($collapsed, $node, $path, $refStack);
        }

        foreach (['if', 'then', 'else', 'not', 'contains'] as $keyword) {
            if (isset($node[$keyword])) {
                $this->warn($path, "{$keyword} was removed because most tool callers ignore advanced JSON Schema branching.");
                unset($node[$keyword]);
            }
        }

        return $node;
    }

    private function collapseNullableUnion(
        array $variants,
        array $document,
        string $path,
        array $refStack,
        bool $requiredProperty,
        int $depth
    ): ?array {
        if (count($variants) !== 2) {
            return null;
        }

        $resolved = [];

        foreach ($variants as $index => $variant) {
            if (!is_array($variant)) {
                return null;
            }

            $resolved[] = $this->inlineLocalRef($variant, $document, "{$path}/{$index}", $refStack);
        }

        $nullIndex = null;

        foreach ($resolved as $index => $variant) {
            if (($variant['type'] ?? null) === 'null') {
                $nullIndex = $index;
                break;
            }
        }

        if ($nullIndex === null) {
            return null;
        }

        $other = $resolved[$nullIndex === 0 ? 1 : 0];

        if (!$requiredProperty && $this->options['convert_nullable_to_optional']) {
            $this->warn(
                $path,
                'Collapsed nullable union into an optional field because omitted values are more portable than explicit nulls.'
            );

            return $this->sanitizeNode($other, $document, $path, $refStack, false, $depth);
        }

        $this->error(
            $path,
            'Required nullable fields are unstable across tool providers. Prefer optional fields or separate explicit state.'
        );

        return $this->sanitizeNode($other, $document, $path, $refStack, true, $depth);
    }

    private function normalizeCommonKeywords(array $node, string $path): array
    {
        foreach (self::ALWAYS_STRIP as $keyword) {
            if (!array_key_exists($keyword, $node)) {
                continue;
            }

            $this->warn($path, "{$keyword} was removed because it is not reliably supported in MCP tool schemas.");
            unset($node[$keyword]);
        }

        if (($this->options['strip_titles'] ?? false) && array_key_exists('title', $node)) {
            unset($node['title']);
        }

        if (($this->options['strip_defaults'] ?? false) && array_key_exists('default', $node)) {
            unset($node['default']);
        }

        if (($this->options['strip_examples'] ?? false)) {
            unset($node['example'], $node['examples']);
        }

        if (isset($node['description'])) {
            if (!is_string($node['description'])) {
                $this->warn($path, 'description must be a string. Casting it.');
                $node['description'] = (string) $node['description'];
            }

            $this->metrics['descriptionBytes'] += strlen($node['description']);
        }

        foreach (['format', 'pattern'] as $keyword) {
            if (!isset($node[$keyword])) {
                continue;
            }

            if (!is_string($node[$keyword]) || $node[$keyword] === '') {
                $this->warn($path, "{$keyword} must be a non-empty string. Removing it.");
                unset($node[$keyword]);
            }
        }

        $node = $this->normalizeEnum($node, $path);

        foreach ([
            'minLength',
            'maxLength',
            'minItems',
            'maxItems',
        ] as $keyword) {
            $node = $this->normalizeNonNegativeInteger($node, $keyword, $path);
        }

        foreach ([
            'minimum',
            'maximum',
            'exclusiveMinimum',
            'exclusiveMaximum',
            'multipleOf',
        ] as $keyword) {
            $node = $this->normalizeNumericKeyword($node, $keyword, $path);
        }

        if (
            isset($node['minLength'], $node['maxLength'])
            && $node['minLength'] > $node['maxLength']
        ) {
            $this->error($path, 'minLength is greater than maxLength. Swapping the values.');
            [$node['minLength'], $node['maxLength']] = [$node['maxLength'], $node['minLength']];
        }

        if (
            isset($node['minItems'], $node['maxItems'])
            && $node['minItems'] > $node['maxItems']
        ) {
            $this->error($path, 'minItems is greater than maxItems. Swapping the values.');
            [$node['minItems'], $node['maxItems']] = [$node['maxItems'], $node['minItems']];
        }

        if (
            isset($node['minimum'], $node['maximum'])
            && $node['minimum'] > $node['maximum']
        ) {
            $this->error($path, 'minimum is greater than maximum. Swapping the values.');
            [$node['minimum'], $node['maximum']] = [$node['maximum'], $node['minimum']];
        }

        if (isset($node['multipleOf']) && $node['multipleOf'] <= 0) {
            $this->error($path, 'multipleOf must be greater than zero. Removing it.');
            unset($node['multipleOf']);
        }

        return $node;
    }

    private function normalizeTypeMetadata(array $node, string $path, bool $requiredProperty): array
    {
        if (($node['nullable'] ?? false) === true) {
            unset($node['nullable']);

            if (!$requiredProperty && $this->options['convert_nullable_to_optional']) {
                $this->warn($path, 'nullable was converted into omitted-field semantics for better cross-provider behavior.');
            } else {
                $this->error($path, 'nullable on required fields is brittle. Prefer optional fields or a dedicated status enum.');
            }
        }

        if (!isset($node['type'])) {
            if (isset($node['properties'])) {
                $node['type'] = 'object';
            } elseif (isset($node['items'])) {
                $node['type'] = 'array';
            } elseif (isset($node['enum']) && $node['enum'] !== []) {
                $node['type'] = $this->inferEnumType($node['enum']);
            }
        }

        if (!isset($node['type'])) {
            return $node;
        }

        if (is_array($node['type'])) {
            $types = array_values(array_unique(array_map('strval', $node['type'])));
            sort($types);

            if (count($types) === 2 && in_array('null', $types, true)) {
                $nonNullType = $types[0] === 'null' ? $types[1] : $types[0];

                if (!$requiredProperty && $this->options['convert_nullable_to_optional']) {
                    $this->warn(
                        $path,
                        "Converted type union [" . implode(', ', $types) . '] into an optional ' . $nonNullType . ' field.'
                    );
                    $node['type'] = $nonNullType;

                    return $node;
                }

                $this->error($path, 'Required nullable unions are not portable. Keeping the non-null type only.');
                $node['type'] = $nonNullType;

                return $node;
            }

            $this->error(
                $path,
                'Multi-type unions are not safe for cross-provider tool calling. Keeping the first type only.'
            );
            $node['type'] = $types[0] ?? 'string';

            return $node;
        }

        if (!is_string($node['type'])) {
            $this->error($path, 'type must be a string. Falling back to string.');
            $node['type'] = 'string';

            return $node;
        }

        $validTypes = ['object', 'array', 'string', 'integer', 'number', 'boolean', 'null'];

        if (!in_array($node['type'], $validTypes, true)) {
            $this->error($path, "Unsupported type {$node['type']}. Falling back to string.");
            $node['type'] = 'string';
        }

        return $node;
    }

    private function sanitizeObjectNode(
        array $node,
        array $document,
        string $path,
        array $refStack,
        int $depth
    ): array {
        $properties = $node['properties'] ?? [];

        if (!is_array($properties) || array_is_list($properties)) {
            $this->error($path, 'Object properties must be an associative array. Resetting to an empty object.');
            $properties = [];
        }

        $required = [];

        if (isset($node['required'])) {
            if (!is_array($node['required']) || !array_is_list($node['required'])) {
                $this->error($path, 'required must be a list of property names. Resetting it.');
            } else {
                foreach ($node['required'] as $propertyName) {
                    if (!is_string($propertyName) || $propertyName === '') {
                        $this->warn($path, 'Found an invalid entry inside required. Skipping it.');
                        continue;
                    }

                    $required[$propertyName] = true;
                }
            }
        }

        $normalizedProperties = [];

        foreach ($properties as $propertyName => $propertySchema) {
            if (!is_string($propertyName) || $propertyName === '') {
                $this->warn($path, 'Skipping an object property with an invalid name.');
                continue;
            }

            if (!is_array($propertySchema)) {
                $this->error(
                    "{$path}/properties/{$propertyName}",
                    'Property schema must be an associative array. Falling back to string.'
                );
                $propertySchema = ['type' => 'string'];
            }

            $this->metrics['propertyCount']++;
            $normalizedProperties[$propertyName] = $this->sanitizeNode(
                $propertySchema,
                $document,
                "{$path}/properties/" . $this->escapeJsonPointerSegment($propertyName),
                $refStack,
                isset($required[$propertyName]),
                $depth + 1
            );
        }

        $requiredNames = array_values(
            array_filter(
                array_keys($required),
                static fn (string $name): bool => array_key_exists($name, $normalizedProperties)
            )
        );
        sort($requiredNames);

        $node['properties'] = $normalizedProperties;

        if ($requiredNames === []) {
            unset($node['required']);
        } else {
            $node['required'] = $requiredNames;
        }

        if (array_key_exists('additionalProperties', $node)) {
            if (is_array($node['additionalProperties'])) {
                if ($this->options['force_closed_objects']) {
                    $this->warn(
                        $path,
                        'additionalProperties schema was replaced with false because dynamic maps are harder to defend in tool input.'
                    );
                    $node['additionalProperties'] = false;
                } else {
                    $node['additionalProperties'] = $this->sanitizeNode(
                        $node['additionalProperties'],
                        $document,
                        "{$path}/additionalProperties",
                        $refStack,
                        false,
                        $depth + 1
                    );
                }
            } elseif (!is_bool($node['additionalProperties'])) {
                $this->error($path, 'additionalProperties must be a boolean or schema. Replacing it with false.');
                $node['additionalProperties'] = false;
            }
        } elseif ($this->options['force_closed_objects']) {
            $node['additionalProperties'] = false;
        }

        return $node;
    }

    private function sanitizeArrayNode(
        array $node,
        array $document,
        string $path,
        array $refStack,
        int $depth
    ): array {
        if (!array_key_exists('items', $node)) {
            $this->error($path, 'Array schema is missing items. Falling back to string items.');
            $node['items'] = ['type' => 'string'];
        } elseif (is_array($node['items']) && array_is_list($node['items'])) {
            $this->error(
                $path,
                'Tuple-style arrays are not reliable across tool providers. Using the first tuple entry as the item schema.'
            );
            $node['items'] = isset($node['items'][0]) && is_array($node['items'][0])
                ? $node['items'][0]
                : ['type' => 'string'];
        } elseif (!is_array($node['items'])) {
            $this->error($path, 'items must be a schema object. Falling back to string items.');
            $node['items'] = ['type' => 'string'];
        }

        $node['items'] = $this->sanitizeNode(
            $node['items'],
            $document,
            "{$path}/items",
            $refStack,
            false,
            $depth + 1
        );

        return $node;
    }

    private function sanitizeScalarNode(array $node, string $path): array
    {
        $type = $node['type'];

        if ($type === 'null') {
            $this->error($path, 'Standalone null fields are not useful in tool inputs. Falling back to string.');
            $node['type'] = 'string';
        }

        if (($node['type'] === 'boolean' || $node['type'] === 'integer' || $node['type'] === 'number')
            && isset($node['pattern'])) {
            $this->warn($path, 'pattern only applies to strings. Removing it.');
            unset($node['pattern']);
        }

        if ($node['type'] !== 'string') {
            unset($node['minLength'], $node['maxLength']);
        }

        if ($node['type'] !== 'array') {
            unset($node['minItems'], $node['maxItems'], $node['items']);
        }

        if ($node['type'] !== 'object') {
            unset($node['properties'], $node['required'], $node['additionalProperties']);
        }

        if ($node['type'] !== 'integer' && $node['type'] !== 'number') {
            unset(
                $node['minimum'],
                $node['maximum'],
                $node['exclusiveMinimum'],
                $node['exclusiveMaximum'],
                $node['multipleOf']
            );
        }

        return $node;
    }

    private function normalizeEnum(array $node, string $path): array
    {
        if (array_key_exists('const', $node) && !array_key_exists('enum', $node)) {
            $node['enum'] = [$node['const']];
        }

        if (!isset($node['enum'])) {
            return $node;
        }

        if (!is_array($node['enum']) || !array_is_list($node['enum'])) {
            $this->error($path, 'enum must be a list. Removing it.');
            unset($node['enum']);

            return $node;
        }

        $uniqueValues = [];
        $seen = [];

        foreach ($node['enum'] as $value) {
            if (!is_scalar($value) && $value !== null) {
                $this->error($path, 'enum values must be scalar or null. Dropping the invalid value.');
                continue;
            }

            $key = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

            if (isset($seen[$key])) {
                continue;
            }

            $seen[$key] = true;
            $uniqueValues[] = $value;
        }

        if ($uniqueValues === []) {
            $this->error($path, 'enum became empty after normalization. Removing it.');
            unset($node['enum']);

            return $node;
        }

        usort(
            $uniqueValues,
            static fn (mixed $left, mixed $right): int => strcmp(
                json_encode($left, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                json_encode($right, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)
            )
        );

        $node['enum'] = $uniqueValues;
        $this->metrics['enumValueCount'] += count($uniqueValues);

        return $node;
    }

    private function normalizeNonNegativeInteger(array $node, string $keyword, string $path): array
    {
        if (!array_key_exists($keyword, $node)) {
            return $node;
        }

        if (!is_int($node[$keyword])) {
            $asString = is_scalar($node[$keyword]) ? (string) $node[$keyword] : null;

            if (
                $asString !== null
                && preg_match('/^\d+$/', $asString) === 1
                && (int) $asString >= 0
            ) {
                $node[$keyword] = (int) $node[$keyword];
            } else {
                $this->warn($path, "{$keyword} must be a non-negative integer. Removing it.");
                unset($node[$keyword]);
            }
        }

        return $node;
    }

    private function normalizeNumericKeyword(array $node, string $keyword, string $path): array
    {
        if (!array_key_exists($keyword, $node)) {
            return $node;
        }

        if (!is_int($node[$keyword]) && !is_float($node[$keyword])) {
            if (is_numeric($node[$keyword])) {
                $node[$keyword] = $node[$keyword] + 0;
            } else {
                $this->warn($path, "{$keyword} must be numeric. Removing it.");
                unset($node[$keyword]);
            }
        }

        return $node;
    }

    private function mergeSchemas(array $base, array $overlay, string $path, array $refStack): array
    {
        foreach ($overlay as $key => $value) {
            if (!array_key_exists($key, $base)) {
                $base[$key] = $value;
                continue;
            }

            if ($key === 'required' && is_array($base[$key]) && is_array($value)) {
                $base[$key] = array_values(array_unique(array_merge($base[$key], $value)));
                sort($base[$key]);
                continue;
            }

            if ($key === 'properties' && is_array($base[$key]) && is_array($value)) {
                foreach ($value as $propertyName => $propertySchema) {
                    if (
                        isset($base[$key][$propertyName])
                        && is_array($base[$key][$propertyName])
                        && is_array($propertySchema)
                    ) {
                        $base[$key][$propertyName] = $this->mergeSchemas(
                            $base[$key][$propertyName],
                            $propertySchema,
                            "{$path}/properties/" . $this->escapeJsonPointerSegment((string) $propertyName),
                            $refStack
                        );
                    } else {
                        $base[$key][$propertyName] = $propertySchema;
                    }
                }
                continue;
            }

            if ($key === 'type' && $base[$key] !== $value) {
                $this->warn($path, 'Conflicting type declarations were merged. The more specific overlay type won.');
            }

            if (is_array($base[$key]) && is_array($value) && $this->isAssoc($base[$key]) && $this->isAssoc($value)) {
                $base[$key] = $this->mergeSchemas($base[$key], $value, "{$path}/{$key}", $refStack);
                continue;
            }

            $base[$key] = $value;
        }

        return $base;
    }

    private function inferEnumType(array $values): string
    {
        $types = [];

        foreach ($values as $value) {
            $types[$this->scalarType($value)] = true;
        }

        if (count($types) === 1) {
            return array_key_first($types) ?? 'string';
        }

        return 'string';
    }

    private function scalarType(mixed $value): string
    {
        return match (true) {
            is_string($value) => 'string',
            is_int($value) => 'integer',
            is_float($value) => 'number',
            is_bool($value) => 'boolean',
            $value === null => 'null',
            default => 'string',
        };
    }

    private function enforceLimits(): void
    {
        if ($this->metrics['maxDepth'] > $this->options['max_depth']) {
            $this->error(
                '$',
                "Schema depth {$this->metrics['maxDepth']} exceeds the safe limit of {$this->options['max_depth']}."
            );
        }

        if ($this->metrics['propertyCount'] > $this->options['max_properties']) {
            $this->error(
                '$',
                "Schema has {$this->metrics['propertyCount']} properties, above the limit of {$this->options['max_properties']}."
            );
        }

        if ($this->metrics['enumValueCount'] > $this->options['max_enum_values']) {
            $this->error(
                '$',
                "Schema has {$this->metrics['enumValueCount']} enum values, above the limit of {$this->options['max_enum_values']}."
            );
        }
    }

    private function canonicalizeNode(array $node): array
    {
        foreach ($node as $key => $value) {
            if (is_array($value)) {
                if ($this->isAssoc($value)) {
                    $node[$key] = $this->canonicalizeNode($value);
                } else {
                    $node[$key] = array_map(
                        fn (mixed $item): mixed => is_array($item) && $this->isAssoc($item)
                            ? $this->canonicalizeNode($item)
                            : $item,
                        $value
                    );
                }
            }
        }

        if (isset($node['required']) && is_array($node['required'])) {
            sort($node['required']);
        }

        if (isset($node['enum']) && is_array($node['enum'])) {
            usort(
                $node['enum'],
                static fn (mixed $left, mixed $right): int => strcmp(
                    json_encode($left, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
                    json_encode($right, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)
                )
            );
        }

        $ordered = [];

        foreach (self::KEY_ORDER as $key) {
            if (array_key_exists($key, $node)) {
                $ordered[$key] = $node[$key];
                unset($node[$key]);
            }
        }

        ksort($node);

        foreach ($node as $key => $value) {
            $ordered[$key] = $value;
        }

        return $ordered;
    }

    private function canonicalJson(array $schema): string
    {
        return (string) json_encode(
            $schema,
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION
        );
    }

    private function validateOptions(): void
    {
        if (!in_array($this->options['target'], self::VALID_TARGETS, true)) {
            throw new McpToolSchemaSanitizationException(
                'Unsupported target. Valid options: ' . implode(', ', self::VALID_TARGETS)
            );
        }

        foreach ([
            'force_closed_objects',
            'inline_local_refs',
            'strip_titles',
            'strip_defaults',
            'strip_examples',
            'convert_nullable_to_optional',
            'sort_keys',
        ] as $booleanOption) {
            if (!is_bool($this->options[$booleanOption])) {
                throw new McpToolSchemaSanitizationException("Option {$booleanOption} must be boolean.");
            }
        }

        foreach (['max_depth', 'max_properties', 'max_enum_values'] as $integerOption) {
            if (!is_int($this->options[$integerOption]) || $this->options[$integerOption] < 1) {
                throw new McpToolSchemaSanitizationException("Option {$integerOption} must be a positive integer.");
            }
        }
    }

    private function warn(string $path, string $message): void
    {
        $this->warnings[] = "{$path}: {$message}";
    }

    private function error(string $path, string $message): void
    {
        $this->errors[] = "{$path}: {$message}";
    }

    private function isAssoc(array $value): bool
    {
        return $value === [] || !array_is_list($value);
    }

    private function escapeJsonPointerSegment(string $segment): string
    {
        return str_replace(['~', '/'], ['~0', '~1'], $segment);
    }
}

/*
This solves the annoying JSON Schema cleanup work you hit when one MCP tool definition needs to survive OpenAI Responses API, Anthropic tools, Gemini function calling, and your own internal agent gateway without random breakage. Built because I kept seeing perfectly reasonable schemas fail in production over small things like local refs, nullable fields, open objects, tuple arrays, or draft keywords that some providers silently ignore.

Use it when you publish PHP MCP servers, Laravel AI backends, Symfony tool gateways, structured output endpoints, or any agent platform where one bad schema can turn into dropped tool calls, silent validation drift, or security problems from overly open input objects. The trick: inline local refs, collapse the safe parts of `allOf`, strip draft-only keywords, normalize enums and numeric limits, close objects by default, and fingerprint the final schema so you can cache it or diff it in CI.

Drop this into any PHP service that emits LLM tool schemas and wants fewer compatibility bugs, safer input contracts, and easier provider migration later. I wrote it to be direct, dependency free, and practical enough that you can fork it, wire it into a deploy pipeline, and trust the result when real traffic starts hitting your tools.
*/
