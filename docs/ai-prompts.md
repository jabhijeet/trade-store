# AI Prompts

The assignment asks that every prompt given to an AI tool is shared in a separate document. This is that document.

**Tool used:** Claude Code (Anthropic), working in this repository. The AI wrote code, tests, diagrams and documentation in response to the prompts below; all commits were made by hand.

## How this document is maintained

- Prompts are copied verbatim, in the order they were given, grouped by working session.
- Each session lists the date and the outcome, so a prompt can be matched to the commits it produced.
- Claude Code can export a session's transcript with the `/export` command, which is the source for the prompts below.

## Sessions

### Earlier sessions (13–14 October 2026): design, implementation, CI and fixes

> **To complete:** paste the prompts from the earlier sessions here, in order. They were not exported automatically
> (the transcripts are stored locally by Claude Code under `~/.claude/projects/`), so they have to be copied from each
> session's `/export` output.

Outcome, by commit:

| Commit | Summary |
|--------|---------|
| `fa8f228` | Liquibase migrations, connection pools, compose stack and UTC clock |
| `6338562` | OWASP scan fix |
| `7479e1c` | Dependency-Check 13.0.0 with a cached NVD database |
| `886659d` | Outbox relay backlog, concurrent first insert and rejection connections |
| `e8374dd` | Patched vulnerable dependencies and a faster OWASP scan |

Prompts:

```text
<paste prompts here>
```

### Session 6 (14 October 2026): requirement check, AI prompts document and remaining features

Outcome: checked the repository against the assignment, added this document, added an end-to-end test of the sample table, and implemented the features the architecture listed as planned (OAuth2/JWT security, per-client rate limiting and request size limits, the MongoDB `trade_views` projection, the `processed_message` purge job, application metrics, structured JSON logs and the Kafka SASL_SSL profile).

Prompt 1 (the assignment document was attached as a file path):

```text
"C:\Users\joshi\Desktop\Trade Assignment - For Architect Lead role.docx"
```

Prompt 2:

```text
do all 3 steps and then we also need to add rate limitor and other remaining implementations mentioned in readme, and other docs
```

The "3 steps" were proposed by the AI in its answer to prompt 1:

1. create this AI prompts document;
2. run the full test suite (`mvn verify`) to confirm everything passes;
3. check each row of the assignment's sample table with a test.
