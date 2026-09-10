---
name: 5xx-investigation
description: Investigate 5xx alarms from CloudWatch API Gateway logs
user_invocable: true
---

# 5xx Alarm Investigation

The user will typically start by giving you a CloudWatch Logs query already
preset for the time frame in question. Each query result represents an incident.
The goal is to classify the incidents into categories based on similarity, and
for each category identify if there is a pre-existing GitHub issue, in which
case a comment recording the incidents needs to be made, or if a new issue needs
to be created.

These incidents will have triggered CloudWatch alarms. When these alarms trip, a
notification is sent to a Google Group, starting a conversation there. The user needs to triage these alarms which is
why they are asking you to do this. Multiple incidents occurring in quick
succession may have been grouped into a single alarm trip. The user will know
how many alarms occurred during the timeframe in question. There should be at
least as many incidents.

## Step 1: Run the query

Run the query. Each result represents an incident.

## Step 2: Investigate each incident

If the query is against the API Gateway logs, you will very likely need to
retrieve the corresponding application log entries, based on the integration
request ID. Once you found an error message or a stack trace, move to the next
step.

## Step 3: Classify

Classify the issue into categories, one per error message or stack trace.

## Step 4: Identify existing issue

For each category, find preexisting issues. Include closed issues in this
search. Print a table that associates each incident category with the issues you
think match that category. Print issues as links so that the user can open them
easily. Work with the user to refine the list. The user needs to explicitly
approve the creation of a new issue for unmatched categories. Don't make any
changes to GitHub yet, this step is just about aggreeing on the contents of that
association table. Once approved, move to the next step.

## Step 5: Report the incidents on the existing issues

Post a short comment to each issue, reporting the recurrence. Keep the comment
short.

## Step 6: Create new issues for unmatched categories

For the title and the description focus on the symptom, the reproduction and the
error message / stack trace. Offer to also post your hypothesis as to the root
cause, and if the user agrees, post that as a special comment. As always, use
the aggreed upon attribution clause in issue descriptions and comments.

## Step 7: Generate the triage summary

The user will paste the triage summary into the Google Groups conversation for
each alarm. They will paste the same summary into each conversation for the time
frame in question. The triage summary should be plain text and have an entry for
each category. Each entry should consist of lines of the form "name: value". For
each category, include a title, the dates and times (local timezone) of the
incidents in that category and links to the issues or issue comments you
created. The dates and times will allow other users to correlate each alarm with
the category should they want to.
