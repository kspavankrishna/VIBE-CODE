#!/usr/bin/env perl
use strict;
use warnings;
use JSON::PP ();
use Getopt::Long qw(GetOptionsFromArray);
use POSIX qw(strftime);

our $VERSION = '1.0.0';

use constant {
    EXIT_OK       => 0,
    EXIT_FINDINGS => 1,
    EXIT_ERROR    => 2,
};

my %SEVERITY_RANK = (low => 1, medium => 2, high => 3, critical => 4);
my %SEVERITY_NAME = reverse %SEVERITY_RANK;

use constant DEFAULT_FAIL_ON          => 'high';
use constant DEFAULT_MAX_DESTROY      => 5;
use constant DEFAULT_MAX_DESTROY_PCT  => 25;
use constant MIN_DESTROYS_FOR_PCT     => 3;
use constant RENAME_SIMILARITY        => 0.80;
use constant RENAME_MIN_ATTRIBUTES    => 3;

# Resource types whose destruction loses data that Terraform cannot recreate.
my @STATEFUL_TYPES = (
    qr/^aws_(db_instance|rds_cluster|rds_cluster_instance|dynamodb_table|s3_bucket|kms_key|ebs_volume|efs_file_system|elasticache_\w+|redshift_cluster|opensearch_domain|elasticsearch_domain|msk_cluster|neptune_cluster|docdb_cluster|secretsmanager_secret|ssm_parameter|backup_vault|glacier_vault|qldb_ledger|timestreamwrite_table|memorydb_cluster|fsx_\w+_file_system|ecr_repository|cognito_user_pool|route53_zone|acm_certificate)$/,
    qr/^google_(sql_database_instance|sql_database|spanner_instance|spanner_database|bigtable_instance|storage_bucket|bigquery_dataset|bigquery_table|compute_disk|kms_crypto_key|kms_key_ring|firestore_database|secret_manager_secret|redis_instance|filestore_instance)$/,
    qr/^azurerm_(mssql_server|mssql_database|postgresql_\w+server|mysql_\w+server|cosmosdb_account|storage_account|storage_container|managed_disk|key_vault|key_vault_key|key_vault_secret|redis_cache|recovery_services_vault|log_analytics_workspace)$/,
    qr/^(kubernetes_persistent_volume(_claim)?|random_password|random_id|tls_private_key)$/,
);

# Boolean and numeric settings where one direction is a security or durability downgrade.
my @PROTECTION_RULES = (
    { types => qr/^aws_(db_instance|rds_cluster)$/, path => ['deletion_protection'],
      bad => sub { is_false($_[0]) }, label => 'deletion protection turned off', severity => 'high' },
    { types => qr/^aws_(db_instance|rds_cluster)$/, path => ['storage_encrypted'],
      bad => sub { is_false($_[0]) }, label => 'storage encryption turned off', severity => 'high', on_create => 1 },
    { types => qr/^aws_(db_instance|redshift_cluster|dms_replication_instance)$/, path => ['publicly_accessible'],
      bad => sub { is_true($_[0]) }, label => 'database or cluster made publicly accessible', severity => 'critical', on_create => 1 },
    { types => qr/^aws_(db_instance|rds_cluster)$/, path => ['backup_retention_period'],
      bad => sub { defined $_[0] && $_[0] =~ /^\d+$/ && $_[0] == 0 }, label => 'automated backups disabled', severity => 'high' },
    { types => qr/^aws_db_instance$/, path => ['multi_az'],
      bad => sub { is_false($_[0]) }, label => 'Multi-AZ failover turned off', severity => 'medium' },
    { types => qr/^aws_ebs_volume$/, path => ['encrypted'],
      bad => sub { is_false($_[0]) }, label => 'EBS volume encryption turned off', severity => 'high', on_create => 1 },
    { types => qr/^aws_kms_key$/, path => ['enable_key_rotation'],
      bad => sub { is_false($_[0]) }, label => 'KMS key rotation turned off', severity => 'medium' },
    { types => qr/^aws_s3_bucket$/, path => ['force_destroy'],
      bad => sub { is_true($_[0]) }, label => 'force_destroy enabled, objects die with the bucket', severity => 'medium' },
    { types => qr/^aws_s3_bucket_versioning$/, path => ['versioning_configuration', 0, 'status'],
      bad => sub { defined $_[0] && $_[0] =~ /^(Suspended|Disabled)$/ }, label => 'bucket versioning suspended', severity => 'high' },
    { types => qr/^aws_s3_bucket_public_access_block$/, path => ['block_public_acls'],
      bad => sub { is_false($_[0]) }, label => 'S3 block_public_acls turned off', severity => 'critical' },
    { types => qr/^aws_s3_bucket_public_access_block$/, path => ['block_public_policy'],
      bad => sub { is_false($_[0]) }, label => 'S3 block_public_policy turned off', severity => 'critical' },
    { types => qr/^aws_s3_bucket_public_access_block$/, path => ['restrict_public_buckets'],
      bad => sub { is_false($_[0]) }, label => 'S3 restrict_public_buckets turned off', severity => 'high' },
    { types => qr/^aws_s3_bucket_public_access_block$/, path => ['ignore_public_acls'],
      bad => sub { is_false($_[0]) }, label => 'S3 ignore_public_acls turned off', severity => 'high' },
    { types => qr/^aws_s3_bucket(_acl)?$/, path => ['acl'],
      bad => sub { defined $_[0] && !ref $_[0] && $_[0] =~ /^(public-read|public-read-write|authenticated-read)$/ },
      label => 'bucket ACL grants public access', severity => 'critical', on_create => 1 },
    { types => qr/^google_sql_database_instance$/, path => ['deletion_protection'],
      bad => sub { is_false($_[0]) }, label => 'deletion protection turned off', severity => 'high' },
    { types => qr/^azurerm_storage_account$/, path => ['allow_nested_items_to_be_public'],
      bad => sub { is_true($_[0]) }, label => 'storage account allows public blobs', severity => 'critical', on_create => 1 },
    { types => qr/^azurerm_storage_account$/, path => ['enable_https_traffic_only'],
      bad => sub { is_false($_[0]) }, label => 'storage account allows plain HTTP', severity => 'high', on_create => 1 },
);

my %SENSITIVE_PORTS = (
    22 => 'SSH', 23 => 'Telnet', 135 => 'RPC', 445 => 'SMB', 1433 => 'MSSQL', 1521 => 'Oracle',
    2375 => 'Docker API', 2379 => 'etcd', 3306 => 'MySQL', 3389 => 'RDP', 5432 => 'Postgres',
    5601 => 'Kibana', 5984 => 'CouchDB', 6379 => 'Redis', 8080 => 'HTTP alt', 9200 => 'Elasticsearch',
    10250 => 'Kubelet', 11211 => 'Memcached', 27017 => 'MongoDB',
);

my $SECRET_NAME = qr/(?:password|passwd|secret|token|private_key|api_key|access_key|client_secret|connection_string)$/i;
my $ADMIN_POLICY_ARN = qr/(?::policy\/|^)(?:AdministratorAccess|PowerUserAccess|IAMFullAccess)$/;

# ---------------------------------------------------------------------------
# small value helpers

sub is_true {
    my ($v) = @_;
    return 0 unless defined $v;
    return $v ? 1 : 0 if JSON::PP::is_bool($v);
    return $v =~ /^(?:true|1)$/i ? 1 : 0;
}

sub is_false {
    my ($v) = @_;
    return 0 unless defined $v;
    return $v ? 0 : 1 if JSON::PP::is_bool($v);
    return $v =~ /^(?:false|0)$/i ? 1 : 0;
}

sub get_path {
    my ($data, @path) = @_;
    for my $seg (@path) {
        return undef unless defined $data;
        if (ref $data eq 'HASH')       { $data = $data->{$seg} }
        elsif (ref $data eq 'ARRAY')   { return undef unless $seg =~ /^\d+$/; $data = $data->[$seg] }
        else                           { return undef }
    }
    return $data;
}

sub as_list {
    my ($v) = @_;
    return () unless defined $v;
    return @$v if ref $v eq 'ARRAY';
    return ($v);
}

# Flatten nested data to "a.b.0.c" => scalar. Booleans become the strings true and false.
sub flatten {
    my ($data, $prefix, $out) = @_;
    $out //= {};
    if (ref $data eq 'HASH') {
        flatten($data->{$_}, defined $prefix ? "$prefix.$_" : $_, $out) for sort keys %$data;
    } elsif (ref $data eq 'ARRAY') {
        flatten($data->[$_], "$prefix.$_", $out) for 0 .. $#$data;
    } elsif (defined $data && defined $prefix) {
        $out->{$prefix} = JSON::PP::is_bool($data) ? ($data ? 'true' : 'false') : "$data";
    }
    return $out;
}

# True when the (possibly nested) after_sensitive marker covers this path.
sub is_marked {
    my ($marker, @path) = @_;
    my $node = $marker;
    for my $seg (@path) {
        return 1 if JSON::PP::is_bool($node) && $node;
        return 0 unless defined $node && ref $node;
        $node = ref $node eq 'HASH' ? $node->{$seg} : ($seg =~ /^\d+$/ ? $node->[$seg] : undef);
    }
    return JSON::PP::is_bool($node) && $node ? 1 : 0;
}

# Walk a structure and call $cb->(\@path, $value) for every scalar leaf.
sub walk_leaves {
    my ($data, $cb, @path) = @_;
    if (ref $data eq 'HASH')      { walk_leaves($data->{$_}, $cb, @path, $_) for sort keys %$data }
    elsif (ref $data eq 'ARRAY')  { walk_leaves($data->[$_], $cb, @path, $_) for 0 .. $#$data }
    elsif (defined $data)         { $cb->(\@path, $data) }
}

sub kind_of {
    my ($actions) = @_;
    my %has = map { $_ => 1 } @$actions;
    return 'replace' if $has{delete} && $has{create};
    return 'delete'  if $has{delete};
    return 'create'  if $has{create};
    return 'update'  if $has{update};
    return 'forget'  if $has{forget};
    return 'read'    if $has{read};
    return 'noop';
}

sub is_stateful {
    my ($type) = @_;
    return scalar grep { $type =~ $_ } @STATEFUL_TYPES;
}

sub glob_to_regex {
    my ($glob) = @_;
    my $re = join '.*', map { quotemeta } split /\*/, $glob, -1;
    return qr/^$re$/;
}

# ---------------------------------------------------------------------------
# plan loading

sub read_plan {
    my ($path) = @_;
    my $raw;
    if ($path eq '-') {
        binmode STDIN, ':raw';
        local $/;
        $raw = <STDIN>;
    } else {
        open my $fh, '<:raw', $path or die "cannot open plan '$path': $!\n";
        local $/;
        $raw = <$fh>;
        close $fh;
    }
    die "plan input is empty\n" unless defined $raw && $raw =~ /\S/;
    die "this is a binary plan file (zip). Run: terraform show -json tfplan > plan.json\n" if $raw =~ /^PK\x03\x04/;

    my $json = JSON::PP->new->utf8->allow_nonref;
    my $plan = eval { $json->decode($raw) };
    if (my $err = $@) {
        my ($first) = split /\n/, $raw, 2;
        my $line = eval { $json->decode($first) };
        die "this looks like 'terraform plan -json' streaming output, not a plan. Use: terraform show -json tfplan\n"
            if ref $line eq 'HASH' && exists $line->{'@level'};
        $err =~ s/\s+at \S+ line \d+.*//s;
        die "plan is not valid JSON: $err\n";
    }
    die "plan JSON must be an object\n" unless ref $plan eq 'HASH';
    unless (exists $plan->{resource_changes}) {
        die "JSON has no resource_changes. It looks like a state document. Use: terraform show -json tfplan\n"
            if exists $plan->{values};
        die "JSON has no resource_changes. Is this the output of terraform show -json on a plan?\n";
    }
    my ($major) = ($plan->{format_version} // '') =~ /^(\d+)\./;
    die "unsupported plan format_version '" . ($plan->{format_version} // 'missing') . "', expected 1.x\n"
        unless defined $major && $major == 1;
    die "the plan is marked errored, Terraform could not finish planning\n" if is_true($plan->{errored});
    return $plan;
}

# ---------------------------------------------------------------------------
# findings

sub add {
    my ($findings, %f) = @_;
    $f{severity} = 'medium' unless exists $SEVERITY_RANK{$f{severity} // ''};
    push @$findings, \%f;
}

sub check_stateful_destroy {
    my ($f, $rc, $kind) = @_;
    return unless $kind eq 'delete' || $kind eq 'replace';
    my $ch = $rc->{change};
    my $type = $rc->{type};
    my $stateful = is_stateful($type);
    return unless $stateful;

    my @notes;
    my $before = $ch->{before} // {};
    my $severity = 'critical';
    if ($kind eq 'replace') {
        my @actions = @{ $ch->{actions} };
        my $order = $actions[0] eq 'delete' ? 'destroy first, then create' : 'create first, then destroy';
        push @notes, "replacement runs $order";
        push @notes, 'destroy first means a window with no resource at all' if $actions[0] eq 'delete';
        push @notes, 'create before destroy cannot reuse a fixed name or identifier, so it may fail midway' if $actions[0] eq 'create';
    }
    if ($type =~ /^aws_(db_instance|rds_cluster)$/) {
        if (is_true($before->{skip_final_snapshot})) {
            push @notes, 'skip_final_snapshot is true, no final snapshot will be taken';
        } elsif (!defined $before->{final_snapshot_identifier} && !is_true($before->{skip_final_snapshot})) {
            push @notes, 'no final_snapshot_identifier is set, apply may fail or lose data';
        }
        push @notes, 'deletion_protection is off in the stored state' if is_false($before->{deletion_protection});
    }
    push @notes, 'force_destroy is true, every object in the bucket will be deleted' if $type eq 'aws_s3_bucket' && is_true($before->{force_destroy});
    push @notes, 'KMS key deletion makes every ciphertext made with it unreadable' if $type =~ /kms_(crypto_)?key$/;
    push @notes, 'a new key or password will be generated, anything holding the old one breaks' if $type =~ /^(random_password|random_id|tls_private_key)$/;
    push @notes, 'a new zone gets different name servers, delegation will break' if $type eq 'aws_route53_zone';
    $severity = 'high' if $type =~ /^(random_id|random_password)$/;

    my @paths = ref $ch->{replace_paths} eq 'ARRAY' ? map { join '.', map { "$_" } @$_ } @{ $ch->{replace_paths} } : ();
    my $why = $rc->{action_reason} ? "reason: $rc->{action_reason}" : '';
    $why .= ($why ? ', ' : '') . 'forced by changes to ' . join(', ', @paths) if @paths;
    unshift @notes, $why if $why;

    add($f,
        rule => 'TFR001', severity => $severity, address => $rc->{address},
        title => ($kind eq 'replace' ? 'Stateful resource will be replaced' : 'Stateful resource will be destroyed'),
        detail => join('; ', @notes) || 'data held by this resource is not recoverable from Terraform',
        fix => $kind eq 'replace'
            ? 'Find the attribute named in the replacement reason and revert it, or plan a managed migration with a snapshot first. Add lifecycle { prevent_destroy = true }.'
            : 'If the removal is deliberate, take a backup first. If it came from a rename, add a moved block. Otherwise restore the resource block.');
}

sub check_replace_reason {
    my ($f, $rc, $kind) = @_;
    return unless $kind eq 'replace';
    return if is_stateful($rc->{type});
    my $ch = $rc->{change};
    my @paths = ref $ch->{replace_paths} eq 'ARRAY' ? map { join '.', map { "$_" } @$_ } @{ $ch->{replace_paths} } : ();
    my $reason = $rc->{action_reason} // 'unspecified';
    my $severity = $reason eq 'replace_because_tainted' ? 'low' : 'medium';
    add($f,
        rule => 'TFR002', severity => $severity, address => $rc->{address},
        title => 'Resource will be replaced, not updated in place',
        detail => "reason: $reason" . (@paths ? '; forced by ' . join(', ', @paths) : ''),
        fix => 'Check whether the forcing attribute change was intended. Anything that depends on this resource will see a new identity.');
}

sub parse_policy {
    my ($v) = @_;
    return $v if ref $v eq 'HASH';
    return undef unless defined $v && !ref $v && $v =~ /^\s*\{/;
    my $doc = eval { JSON::PP->new->utf8->decode($v) };
    return ref $doc eq 'HASH' ? $doc : undef;
}

# Returns signature => {severity, text} for the dangerous Allow statements in a policy document.
sub policy_grants {
    my ($doc) = @_;
    my %grants;
    return \%grants unless $doc;
    for my $st (as_list($doc->{Statement})) {
        next unless ref $st eq 'HASH' && ($st->{Effect} // '') eq 'Allow';
        my @actions   = as_list($st->{Action});
        my @resources = as_list($st->{Resource});
        my $conditioned = ref $st->{Condition} eq 'HASH' && %{ $st->{Condition} };
        my $principal = $st->{Principal};
        my @principals = ref $principal eq 'HASH' ? map { as_list($_) } values %$principal : as_list($principal);

        if (grep { defined $_ && $_ eq '*' } @principals and !$conditioned) {
            my $sig = 'principal:*|' . join(',', sort @actions) . '|' . join(',', sort @resources);
            $grants{$sig} = { severity => 'critical', text => 'Allow to Principal "*" with no Condition: anyone on the internet or in any account' };
        }
        if ($st->{NotAction}) {
            my $sig = 'notaction|' . join(',', sort as_list($st->{NotAction})) . '|' . join(',', sort @resources);
            $grants{$sig} = { severity => 'high', text => 'Allow with NotAction grants everything except the listed actions' };
        }
        my $all_resources = grep { defined $_ && $_ eq '*' } @resources;
        for my $a (@actions) {
            next unless defined $a;
            if ($a eq '*' && $all_resources) {
                $grants{"admin|$a"} = { severity => 'critical', text => 'Action "*" on Resource "*": full administrator' };
            } elsif ($a eq '*') {
                $grants{"allactions|" . join(',', sort @resources)} = { severity => 'high', text => 'Action "*" on ' . join(', ', @resources) };
            } elsif ($a =~ /^([\w-]+):\*$/ && $all_resources) {
                $grants{"svc|$a"} = { severity => ($1 =~ /^(iam|sts|kms|organizations)$/i ? 'critical' : 'high'),
                                      text => qq{Action "$a" on Resource "*": every $1 API on every resource} };
            } elsif ($a =~ /^iam:(PassRole|CreateAccessKey|AttachRolePolicy|PutRolePolicy|UpdateAssumeRolePolicy|CreatePolicyVersion)$/i && $all_resources) {
                $grants{"esc|$a"} = { severity => 'high', text => qq{Action "$a" on Resource "*": a known privilege escalation path} };
            }
        }
    }
    return \%grants;
}

sub check_iam {
    my ($f, $rc, $kind) = @_;
    return if $kind eq 'delete' || $kind eq 'noop' || $kind eq 'read' || $kind eq 'forget';
    my $ch = $rc->{change};
    my $after = $ch->{after};
    return unless ref $after eq 'HASH';

    if ($rc->{type} =~ /^aws_iam_(?:role_|user_|group_)?policy_attachment$/ || $rc->{type} eq 'aws_iam_policy_attachment') {
        my $arn = $after->{policy_arn};
        if (defined $arn && !ref $arn && $arn =~ $ADMIN_POLICY_ARN) {
            my $was = get_path($ch->{before}, 'policy_arn');
            unless (defined $was && $was eq $arn) {
                add($f, rule => 'TFR003', severity => 'critical', address => $rc->{address},
                    title => 'Broad managed IAM policy attached',
                    detail => "policy_arn $arn gives wide account access",
                    fix => 'Attach a scoped policy that lists only the actions this principal needs.');
            }
        }
    }

    for my $attr (qw(policy assume_role_policy inline_policy)) {
        my $new = $after->{$attr};
        next unless defined $new;
        my @new_docs = $attr eq 'inline_policy' && ref $new eq 'ARRAY'
            ? map { $_->{policy} } grep { ref $_ eq 'HASH' } @$new : ($new);
        my @old_docs = ref $ch->{before} eq 'HASH' && defined $ch->{before}{$attr}
            ? ($attr eq 'inline_policy' && ref $ch->{before}{$attr} eq 'ARRAY'
                ? map { $_->{policy} } grep { ref $_ eq 'HASH' } @{ $ch->{before}{$attr} } : ($ch->{before}{$attr}))
            : ();
        my %old;
        %old = (%old, %{ policy_grants(parse_policy($_)) }) for @old_docs;
        for my $doc (@new_docs) {
            my $unknown = ref $ch->{after_unknown} eq 'HASH' && $ch->{after_unknown}{$attr};
            if (!defined $doc && $unknown) {
                add($f, rule => 'TFR003', severity => 'low', address => $rc->{address},
                    title => "Policy in '$attr' is unknown until apply",
                    detail => 'the document is built from values Terraform cannot see yet, so it cannot be checked here',
                    fix => 'Review the rendered policy after apply, or split the plan with -target in a staging workspace.');
                next;
            }
            my $parsed = parse_policy($doc);
            next unless $parsed;
            my $grants = policy_grants($parsed);
            for my $sig (sort keys %$grants) {
                next if exists $old{$sig};
                my $g = $grants->{$sig};
                add($f, rule => 'TFR003', severity => $g->{severity}, address => $rc->{address},
                    title => "New dangerous permission in '$attr'",
                    detail => $g->{text},
                    fix => 'Replace wildcards with the specific actions and ARNs required. Add a Condition if a wildcard is unavoidable.');
            }
        }
    }
}

# Expand a port expression to [low, high]. Returns undef when it cannot be read.
sub port_range {
    my ($expr) = @_;
    return undef unless defined $expr;
    return [0, 65535] if $expr eq '*' || $expr eq 'all' || $expr eq '-1';
    return [$1, $2] if $expr =~ /^(\d+)\s*-\s*(\d+)$/;
    return [$1, $1] if $expr =~ /^(\d+)$/;
    return undef;
}

sub exposure_text {
    my ($lo, $hi, $all_protocols) = @_;
    return 'all ports and protocols' if $all_protocols || ($lo == 0 && $hi == 65535);
    my @hit = map { "$_ ($SENSITIVE_PORTS{$_})" } grep { $_ >= $lo && $_ <= $hi } sort { $a <=> $b } keys %SENSITIVE_PORTS;
    return @hit ? join(', ', @hit) : undef;
}

# Each rule is reduced to a signature so a rule that was already open before is not reported again.
sub open_ingress_rules {
    my ($type, $obj) = @_;
    my @out;
    return @out unless ref $obj eq 'HASH';
    my $world = sub { grep { defined $_ && ($_ eq '0.0.0.0/0' || $_ eq '::/0' || $_ eq '*' || $_ eq 'Internet' || $_ eq 'Any') } @_ };

    if ($type eq 'aws_security_group') {
        for my $r (as_list($obj->{ingress})) {
            next unless ref $r eq 'HASH';
            next unless $world->(as_list($r->{cidr_blocks}), as_list($r->{ipv6_cidr_blocks}));
            push @out, { lo => $r->{from_port} // 0, hi => $r->{to_port} // 65535, all => (($r->{protocol} // '') eq '-1') };
        }
    } elsif ($type eq 'aws_security_group_rule') {
        if (($obj->{type} // '') eq 'ingress' && $world->(as_list($obj->{cidr_blocks}), as_list($obj->{ipv6_cidr_blocks}))) {
            push @out, { lo => $obj->{from_port} // 0, hi => $obj->{to_port} // 65535, all => (($obj->{protocol} // '') eq '-1') };
        }
    } elsif ($type eq 'aws_vpc_security_group_ingress_rule') {
        if ($world->($obj->{cidr_ipv4}, $obj->{cidr_ipv6})) {
            push @out, { lo => $obj->{from_port} // 0, hi => $obj->{to_port} // 65535, all => (($obj->{ip_protocol} // '') eq '-1') };
        }
    } elsif ($type eq 'google_compute_firewall') {
        if (uc($obj->{direction} // 'INGRESS') eq 'INGRESS' && $world->(as_list($obj->{source_ranges}))) {
            for my $a (as_list($obj->{allow})) {
                next unless ref $a eq 'HASH';
                my @ports = as_list($a->{ports});
                if (!@ports) { push @out, { lo => 0, hi => 65535, all => 1 }; next }
                for my $p (@ports) { my $r = port_range($p) or next; push @out, { lo => $r->[0], hi => $r->[1], all => 0 } }
            }
        }
    } elsif ($type eq 'azurerm_network_security_rule') {
        if (($obj->{direction} // '') eq 'Inbound' && ($obj->{access} // '') eq 'Allow'
            && $world->($obj->{source_address_prefix}, as_list($obj->{source_address_prefixes}))) {
            for my $p ((defined $obj->{destination_port_range} ? ($obj->{destination_port_range}) : ()), as_list($obj->{destination_port_ranges})) {
                my $r = port_range($p) or next;
                push @out, { lo => $r->[0], hi => $r->[1], all => 0 };
            }
        }
    }
    return map { { %$_, sig => join(':', $_->{lo}, $_->{hi}, $_->{all}) } } @out;
}

sub check_network {
    my ($f, $rc, $kind) = @_;
    return if $kind =~ /^(?:delete|noop|read|forget)$/;
    my $ch = $rc->{change};
    my %was = map { $_->{sig} => 1 } open_ingress_rules($rc->{type}, $ch->{before});
    for my $rule (open_ingress_rules($rc->{type}, $ch->{after})) {
        next if $was{ $rule->{sig} };
        my $what = exposure_text($rule->{lo}, $rule->{hi}, $rule->{all});
        next unless defined $what;
        add($f, rule => 'TFR004', severity => ($rule->{all} || ($rule->{lo} == 0 && $rule->{hi} == 65535) ? 'critical' : 'high'),
            address => $rc->{address}, title => 'Ingress opened to the whole internet',
            detail => "world reachable: $what",
            fix => 'Restrict the source to a known CIDR, a security group reference or a VPN range. Use a bastion or SSM for admin access.');
    }
}

sub check_protection {
    my ($f, $rc, $kind) = @_;
    return if $kind =~ /^(?:delete|noop|read|forget)$/;
    my $ch = $rc->{change};
    for my $rule (@PROTECTION_RULES) {
        next unless $rc->{type} =~ $rule->{types};
        my $new = get_path($ch->{after}, @{ $rule->{path} });
        next unless defined $new && $rule->{bad}->($new);
        my $old = get_path($ch->{before}, @{ $rule->{path} });
        my $was_bad = defined $old && $rule->{bad}->($old);
        next if $was_bad;
        next if $kind eq 'create' && !$rule->{on_create};
        add($f, rule => 'TFR005', severity => $rule->{severity}, address => $rc->{address},
            title => ucfirst($rule->{label}),
            detail => join('.', @{ $rule->{path} }) . ($kind eq 'create' ? ' is set that way on creation' : ' changes from ' . (defined $old ? $old : 'unset') . " to $new"),
            fix => 'Keep the protective value. If the change is deliberate, record a time limited waiver with a reason.');
    }
}

sub check_secrets {
    my ($f, $rc, $kind) = @_;
    return if $kind =~ /^(?:delete|noop|read|forget)$/;
    my $ch = $rc->{change};
    return unless ref $ch->{after} eq 'HASH';
    return if $rc->{type} =~ /^(?:random_|tls_)/;
    my @exposed;
    walk_leaves($ch->{after}, sub {
        my ($path, $value) = @_;
        return if ref $value || JSON::PP::is_bool($value);
        my $leaf = $path->[-1];
        return if $leaf =~ /^\d+$/ || $leaf !~ $SECRET_NAME;
        return unless length "$value";
        return if is_marked($ch->{after_sensitive}, @$path);
        return if $leaf =~ /(?:_arn|_id|_name|_url|_path|_file|_version|_length|_rotation\w*)$/i;
        push @exposed, join('.', @$path);
    });
    return unless @exposed;
    add($f, rule => 'TFR006', severity => 'high', address => $rc->{address},
        title => 'Secret-like attribute is not marked sensitive',
        detail => 'literal value stored in plan and state in clear text at: ' . join(', ', @exposed[0 .. ($#exposed > 4 ? 4 : $#exposed)]) . (@exposed > 5 ? sprintf(' (+%d more)', @exposed - 5) : ''),
        fix => 'Read the value from a secrets manager or an ephemeral resource, mark the variable sensitive and rotate the value if this plan was ever logged.');
}

sub check_blast_radius {
    my ($f, $changes, $opt) = @_;
    my ($destroys, $managed) = (0, 0);
    my %by_module;
    for my $rc (@$changes) {
        next unless ($rc->{mode} // '') eq 'managed';
        $managed++;
        my $k = kind_of($rc->{change}{actions});
        next unless $k eq 'delete' || $k eq 'replace';
        $destroys++;
        my ($mod) = ($rc->{address} // '') =~ /^((?:module\.[^.]+(?:\[[^\]]*\])?\.)+)/;
        $by_module{ $mod // 'root' }++;
    }
    return ($destroys, $managed) unless $destroys;
    my $pct = $managed ? 100 * $destroys / $managed : 0;
    if ($destroys > $opt->{max_destroy}) {
        add($f, rule => 'TFR007', severity => 'high', address => '(plan)',
            title => 'Too many resources destroyed in one plan',
            detail => sprintf('%d destroys or replacements, limit is %d', $destroys, $opt->{max_destroy}),
            fix => 'Split the change into smaller applies, or raise --max-destroy for a reviewed teardown.');
    } elsif ($destroys >= MIN_DESTROYS_FOR_PCT && $pct > $opt->{max_destroy_pct}) {
        add($f, rule => 'TFR007', severity => 'high', address => '(plan)',
            title => 'Large share of managed resources destroyed',
            detail => sprintf('%d of %d managed resources (%.0f%%), limit is %d%%', $destroys, $managed, $pct, $opt->{max_destroy_pct}),
            fix => 'A wrong workspace, backend or variable file is the usual cause. Confirm the target before applying.');
    }
    return ($destroys, $managed, \%by_module);
}

sub check_drift {
    my ($f, $plan) = @_;
    for my $rc (@{ $plan->{resource_drift} // [] }) {
        next unless kind_of($rc->{change}{actions}) eq 'delete';
        next unless ($rc->{mode} // 'managed') eq 'managed';
        add($f, rule => 'TFR008', severity => (is_stateful($rc->{type}) ? 'high' : 'medium'), address => $rc->{address},
            title => 'Resource was deleted outside Terraform',
            detail => 'the refresh found it missing, so this plan will create it again with empty contents',
            fix => 'Decide whether the deletion was intended. If so remove the block, otherwise restore data from backup after apply.');
    }
}

# A delete in one address plus a create of the same shape in another is almost always a rename.
sub check_probable_renames {
    my ($f, $changes) = @_;
    my (@deleted, @created);
    for my $rc (@$changes) {
        next unless ($rc->{mode} // '') eq 'managed';
        my $k = kind_of($rc->{change}{actions});
        push @deleted, $rc if $k eq 'delete';
        push @created, $rc if $k eq 'create';
    }
    my %taken;
    my @pairs;
    for my $d (@deleted) {
        my $before = flatten($d->{change}{before});
        for my $c (@created) {
            next unless $c->{type} eq $d->{type} && $c->{address} ne $d->{address};
            my $after = flatten($c->{change}{after});
            my $unknown = flatten($c->{change}{after_unknown} // {});
            my @known = grep { !exists $unknown->{$_} } keys %$after;
            next if @known < RENAME_MIN_ATTRIBUTES;
            my $same = grep { defined $before->{$_} && $before->{$_} eq $after->{$_} } @known;
            my $score = $same / @known;
            push @pairs, [$score, $d->{address}, $c->{address}] if $score >= RENAME_SIMILARITY;
        }
    }
    for my $p (sort { $b->[0] <=> $a->[0] || $a->[1] cmp $b->[1] || $a->[2] cmp $b->[2] } @pairs) {
        my ($score, $from, $to) = @$p;
        next if $taken{"f$from"}++ || $taken{"t$to"}++;
        add($f, rule => 'TFR009', severity => 'high', address => $from,
            title => 'Looks like a rename that will destroy and recreate',
            detail => sprintf('%.0f%% of configured attributes match the new resource %s', 100 * $score, $to),
            fix => "Add: moved { from = $from  to = $to } and re-plan. The resource should then show no change.");
    }
}

# ---------------------------------------------------------------------------
# waivers

sub load_waivers {
    my ($path, $today) = @_;
    return [] unless defined $path;
    open my $fh, '<:raw', $path or die "cannot open waivers '$path': $!\n";
    local $/;
    my $raw = <$fh>;
    close $fh;
    my $doc = eval { JSON::PP->new->utf8->decode($raw) };
    die "waivers file is not valid JSON\n" if $@;
    my $list = ref $doc eq 'HASH' ? $doc->{waivers} : $doc;
    die "waivers file needs a 'waivers' array\n" unless ref $list eq 'ARRAY';
    my $i = 0;
    my @out;
    for my $w (@$list) {
        $i++;
        die "waiver #$i must be an object\n" unless ref $w eq 'HASH';
        for my $need (qw(rule address expires reason)) {
            die "waiver #$i is missing '$need'. Every waiver needs a rule, address, expiry date and reason.\n"
                unless defined $w->{$need} && length $w->{$need};
        }
        die "waiver #$i has expires '$w->{expires}', expected YYYY-MM-DD\n" unless $w->{expires} =~ /^\d{4}-\d{2}-\d{2}$/;
        push @out, { %$w, re => glob_to_regex($w->{address}), expired => ($w->{expires} lt $today ? 1 : 0), used => 0 };
    }
    return \@out;
}

sub apply_waivers {
    my ($findings, $waivers) = @_;
    my (@kept, @waived);
    FINDING: for my $f (@$findings) {
        for my $w (@$waivers) {
            next unless $w->{rule} eq $f->{rule} || $w->{rule} eq '*';
            next unless $f->{address} =~ $w->{re};
            $w->{used}++;
            next if $w->{expired};
            push @waived, { %$f, waiver => $w };
            next FINDING;
        }
        push @kept, $f;
    }
    return (\@kept, \@waived);
}

# ---------------------------------------------------------------------------
# analysis and rendering

sub analyse {
    my ($plan, $opt) = @_;
    my @findings;
    my $changes = $plan->{resource_changes};
    my %counts = (create => 0, update => 0, delete => 0, replace => 0, noop => 0, read => 0, forget => 0);

    for my $rc (@$changes) {
        next unless ref $rc eq 'HASH' && ref $rc->{change} eq 'HASH';
        $rc->{change}{actions} = [] unless ref $rc->{change}{actions} eq 'ARRAY';
        my $kind = kind_of($rc->{change}{actions});
        $counts{$kind}++;
        next if ($rc->{mode} // 'managed') ne 'managed';
        next if $kind eq 'noop' || $kind eq 'read';
        check_stateful_destroy(\@findings, $rc, $kind);
        check_replace_reason(\@findings, $rc, $kind);
        check_iam(\@findings, $rc, $kind);
        check_network(\@findings, $rc, $kind);
        check_protection(\@findings, $rc, $kind);
        check_secrets(\@findings, $rc, $kind);
    }
    my ($destroys, $managed, $by_module) = check_blast_radius(\@findings, $changes, $opt);
    check_drift(\@findings, $plan);
    check_probable_renames(\@findings, $changes);

    if (defined $plan->{complete} && is_false($plan->{complete})) {
        add(\@findings, rule => 'TFR010', severity => 'medium', address => '(plan)',
            title => 'Plan is incomplete',
            detail => 'Terraform reports complete=false: some changes need a second plan after this apply',
            fix => 'Expect a follow up plan. Do not treat a clean first apply as the end state.');
    }

    @findings = sort {
        $SEVERITY_RANK{ $b->{severity} } <=> $SEVERITY_RANK{ $a->{severity} }
            || $a->{address} cmp $b->{address} || $a->{rule} cmp $b->{rule}
    } @findings;

    return { findings => \@findings, counts => \%counts, destroys => $destroys // 0, managed => $managed // 0 };
}

sub verdict {
    my ($kept, $fail_on) = @_;
    my $threshold = $SEVERITY_RANK{$fail_on};
    return (grep { $SEVERITY_RANK{ $_->{severity} } >= $threshold } @$kept) ? 'FAIL' : 'PASS';
}

sub render_text {
    my ($plan, $res, $kept, $waived, $waivers, $opt, $verdict) = @_;
    my $c = $res->{counts};
    my $out = '';
    $out .= "Terraform Plan Risk Gate $VERSION\n";
    $out .= sprintf("terraform %s, %d to create, %d to update, %d to replace, %d to destroy\n",
        $plan->{terraform_version} // 'unknown', $c->{create}, $c->{update}, $c->{replace}, $c->{delete});
    $out .= "verdict: $verdict (fail on $opt->{fail_on} or worse), " . scalar(@$kept) . ' finding(s), ' . scalar(@$waived) . " waived\n";
    for my $f (@$kept) {
        $out .= sprintf("\n[%s] %s %s\n  %s\n  why: %s\n  fix: %s\n", uc $f->{severity}, $f->{rule}, $f->{address}, $f->{title}, $f->{detail}, $f->{fix});
    }
    if (@$waived) {
        $out .= "\nwaived:\n";
        $out .= sprintf("  %s %s until %s: %s\n", $_->{rule}, $_->{address}, $_->{waiver}{expires}, $_->{waiver}{reason}) for @$waived;
    }
    for my $w (grep { $_->{expired} } @$waivers) {
        $out .= sprintf("\nEXPIRED WAIVER ignored: %s %s expired %s (%s)\n", $w->{rule}, $w->{address}, $w->{expires}, $w->{reason});
    }
    for my $w (grep { !$_->{used} && !$_->{expired} } @$waivers) {
        $out .= sprintf("\nstale waiver, matched nothing: %s %s (expires %s)\n", $w->{rule}, $w->{address}, $w->{expires});
    }
    return $out;
}

sub render_markdown {
    my ($plan, $res, $kept, $waived, $waivers, $opt, $verdict) = @_;
    my $c = $res->{counts};
    my $out = "### Terraform Plan Risk Gate: $verdict\n\n";
    $out .= sprintf("%d create, %d update, %d replace, %d destroy. %d finding(s), %d waived.\n\n",
        $c->{create}, $c->{update}, $c->{replace}, $c->{delete}, scalar(@$kept), scalar(@$waived));
    if (@$kept) {
        $out .= "| Severity | Rule | Resource | Problem |\n|---|---|---|---|\n";
        for my $f (@$kept) {
            (my $d = $f->{detail}) =~ s/\|/\\|/g;
            $out .= sprintf("| %s | %s | `%s` | %s: %s |\n", $f->{severity}, $f->{rule}, $f->{address}, $f->{title}, $d);
        }
        $out .= "\n<details><summary>How to fix</summary>\n\n";
        $out .= sprintf("- `%s` %s: %s\n", $_->{address}, $_->{rule}, $_->{fix}) for @$kept;
        $out .= "\n</details>\n";
    }
    for my $w (grep { $_->{expired} } @$waivers) {
        $out .= sprintf("\n> Expired waiver ignored: `%s` `%s` expired %s.\n", $w->{rule}, $w->{address}, $w->{expires});
    }
    return $out;
}

sub render_json {
    my ($plan, $res, $kept, $waived, $waivers, $opt, $verdict) = @_;
    my $doc = {
        tool => 'TerraformPlanRiskGate', version => $VERSION, verdict => $verdict, fail_on => $opt->{fail_on},
        terraform_version => $plan->{terraform_version}, counts => $res->{counts},
        findings => $kept,
        waived => [ map { my %c = %$_; my $w = delete $c{waiver}; { %c, waiver => { map { $_ => $w->{$_} } qw(rule address expires reason) } } } @$waived ],
    };
    $doc->{expired_waivers} = [ map { { rule => $_->{rule}, address => $_->{address}, expires => $_->{expires}, reason => $_->{reason} } } grep { $_->{expired} } @$waivers ];
    return JSON::PP->new->utf8->canonical->pretty->encode($doc);
}

sub usage {
    return <<'USAGE';
Usage: TerraformPlanRiskGate.pl [options] PLAN.json | -

Reads the output of `terraform show -json tfplan` and fails the build when the plan
destroys state, widens access or lowers a protection.

Options:
  --fail-on LEVEL         low, medium, high or critical (default high)
  --format FORMAT         text, markdown or json (default text)
  --waivers FILE          JSON file of time limited waivers
  --max-destroy N         most destroys or replacements allowed (default 5)
  --max-destroy-pct N     most percent of managed resources destroyed (default 25)
  --today YYYY-MM-DD      date used to expire waivers (default: today)
  --output FILE           write the report here as well as stdout
  --version, --help

Exit codes: 0 pass, 1 findings at or above --fail-on, 2 bad input or usage.
USAGE
}

sub run {
    my (@argv) = @_;
    my %opt = (fail_on => DEFAULT_FAIL_ON, format => 'text', max_destroy => DEFAULT_MAX_DESTROY,
               max_destroy_pct => DEFAULT_MAX_DESTROY_PCT);
    my ($help, $version);
    my $ok = GetOptionsFromArray(\@argv,
        'fail-on=s' => \$opt{fail_on}, 'format=s' => \$opt{format}, 'waivers=s' => \$opt{waivers},
        'max-destroy=i' => \$opt{max_destroy}, 'max-destroy-pct=i' => \$opt{max_destroy_pct},
        'today=s' => \$opt{today}, 'output=s' => \$opt{output}, 'help' => \$help, 'version' => \$version);
    if ($help)    { print usage(); return EXIT_OK }
    if ($version) { print "$VERSION\n"; return EXIT_OK }
    return _fail(usage()) unless $ok && @argv == 1;
    return _fail("--fail-on must be one of low, medium, high, critical\n") unless exists $SEVERITY_RANK{ $opt{fail_on} };
    return _fail("--format must be text, markdown or json\n") unless $opt{format} =~ /^(?:text|markdown|json)$/;
    $opt{today} //= strftime('%Y-%m-%d', localtime);
    return _fail("--today must look like YYYY-MM-DD\n") unless $opt{today} =~ /^\d{4}-\d{2}-\d{2}$/;

    my $report = eval {
        my $plan    = read_plan($argv[0]);
        my $waivers = load_waivers($opt{waivers}, $opt{today});
        my $res     = analyse($plan, \%opt);
        my ($kept, $waived) = apply_waivers($res->{findings}, $waivers);
        my $verdict = verdict($kept, $opt{fail_on});
        my $render = { text => \&render_text, markdown => \&render_markdown, json => \&render_json }->{ $opt{format} };
        [ $render->($plan, $res, $kept, $waived, $waivers, \%opt, $verdict), $verdict ];
    };
    if (my $err = $@) {
        $err =~ s/ at \S+ line \d+\.?\n?$//;
        $err .= "\n" unless $err =~ /\n$/;
        return _fail("error: $err");
    }
    my ($text, $verdict) = @$report;
    print $text;
    if (defined $opt{output}) {
        open my $out, '>:raw', $opt{output} or return _fail("error: cannot write '$opt{output}': $!\n");
        print {$out} $text;
        close $out or return _fail("error: cannot finish writing '$opt{output}': $!\n");
    }
    return $verdict eq 'FAIL' ? EXIT_FINDINGS : EXIT_OK;
}

sub _fail {
    print STDERR @_;
    return EXIT_ERROR;
}

exit run(@ARGV) unless caller;
1;
