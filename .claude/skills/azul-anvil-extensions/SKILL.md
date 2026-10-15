---
description: "Recompile the set of known file name extensions in the AnVIL metadata plugin by sampling the file names in an AnVIL catalog."
---

# Recompile the AnVIL extension set

Use this skill when the `_extensions` set in `azul.plugins.metadata.anvil`
needs to be checked or extended, typically after an AnVIL release brings new
snapshots.

## What it is for

`_split_extension` divides a file name into a stem and an extension, so that a
download can carry a digest between the two, as in
`NA19189.chr2.hc-9c4a1f3e2b7d5a60.vcf.gz`. The last dot-separated component is
always taken to be an extension. Earlier components are taken only while they
appear in `_extensions`, so the set decides where a multi-part extension such
as `.vcf.gz` or `.cram.crai` begins.

A name component that is wrongly in the set moves part of the stem into the
extension. One that is wrongly absent leaves the digest inside what a reader
would call the extension. Neither affects uniqueness, which comes from the
digest, so both are cosmetic.

Do not use `anvil_file.file_format` for this. It is derived from the file name
by a flawed heuristic, and holds the literal `Other` for about a seventh of the
files.

## Where to run it

The Azul index, not BigQuery, and not the service API.

- The index is the right population. A manifest names only files that are
  indexed, and orphan files never are, so the index excludes exactly what the
  split never sees.

- The service API is the wrong population. An unauthenticated request sees only
  public snapshots, about a fifth of the files in `anvil15`.

Querying the index directly requires the VPN for the deployment's VPC, AWS
credentials, and `AZUL_OPENSEARCH_ENDPOINT` set to the domain's VPC endpoint,
which `config.opensearch_endpoint` does not provide outside a deployed lambda.

## Procedure

1. Aggregate on the segment *before* the last one. The last segment needs no
   decision, so only these matter.

2. Aggregate on the segment before that, to confirm that three-part extensions
   such as `.vcf.gz.tbi` resolve through tokens the set already holds.

3. Classify each observed token. This is judgement, not measurement: the counts
   say how often a token occurs, not whether it names a file type. Weigh a
   misclassification by its count.

4. Validate the result over every distinct name, not a sample. For each name,
   split it and record the tail of the stem. A stem tail that is absent from
   the set is a candidate for inclusion; inspect the ones with the highest
   counts.

5. Record in the comment above the set which release it was compiled from.

## Implementation notes

### Reaching the index

```python
import os
host = 'vpc-azul-index-<deployment>-<id>.us-east-1.es.amazonaws.com'
os.environ['AZUL_OPENSEARCH_ENDPOINT'] = host + ':443'
from azul.opensearch import OpenSearchClientFactory
es = OpenSearchClientFactory.get()
```

Obtain the host with

```
aws opensearch describe-domain --domain-name azul-index-<deployment> \
    --query 'DomainStatus.Endpoints.vpc' --output text
```

If the name resolves but a connection times out, the VPN is up but routing the
wrong VPC. Compare `netstat -rn -f inet` against the address the name resolves
to.

### Aggregating on the nth segment from the end

```python
F = 'contents.files.file_name.keyword'
script = '''
String n = doc['%s'].size() == 0 ? '' : doc['%s'].value;
int end = n.length();
int k = %d;
while (k > 0) {
    int i = n.lastIndexOf('.', end - 1);
    if (i < 0) return '(none)';
    if (k == 1) return n.substring(i + 1, end).toLowerCase();
    end = i;
    k--;
}
return '(none)';
''' % (F, F, 2)
body = {'size': 0, 'aggs': {'second': {'terms': {'script': {'source': script},
                                                 'size': 60}}}}
```

Pass no `query`, so that managed-access snapshots are included.

### Enumerating every distinct name

A composite aggregation on `contents.files.file_name.keyword` yields each
distinct name once, with the number of files carrying it. Page it with
`after_key`. In `anvil15` that is about 2.5 million names over 3.1 million
files, in roughly 250 pages of 10,000.

## What the last run found

Against `anvil15` on `anvilprod`, over all 3,145,469 files:

- Six tokens were missing from a set compiled from the top thirty alone:
  `yaml`, `idat`, `fasta`, `gff3`, `fa` and `svs`.

- The full validation found one more, `g`, which marks a gVCF as in
  `.g.vcf.gz`. All 66,413 names where `g` precedes `.vcf` are gVCFs, and the
  5,163 others are `.g.phased.vcf.gz`, where `phased` stops the walk first.

- The tokens that must stay out, by how much a mistake would cost: `md`
  (142,245 files, as in `.md.bam`), `final`, `out`, `genotyped`,
  `hard-filtered`, `patched`, `sorted`, `stats`, `dist`, `eh`.

- 204 distinct extensions were produced, none implausible, so no token in the
  set was eating stems.
