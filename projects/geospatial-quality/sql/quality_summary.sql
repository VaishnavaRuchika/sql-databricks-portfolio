-- Counts violations separately from affected source records.
SELECT rule, COUNT(*) AS issue_count,
       COUNT(DISTINCT source_row) AS affected_rows
FROM issues
GROUP BY rule
ORDER BY issue_count DESC, rule;
