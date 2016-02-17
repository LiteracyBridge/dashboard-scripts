COPY (
  select title, package as "Package", category, "order", format, 
    length_minutes, minutes_per_tb, eff_completions_per_tb, total_minutes_played,
    total_completions, total_effective_completions, partial_percentage, 
    tbs, pkgtbs, pct_tb_completions as "TB%"
  from topmsgsbypkgall_s
  where project = :'prj' and contentpackage = :'pkg'
) to STDOUT (FORMAT csv, HEADER true);

