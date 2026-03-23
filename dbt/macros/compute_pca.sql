{# ────────────────────────────────────────────────────────────────────────── #}
{# PCA composite performance scores — Manage & Scariano (2013).               #}
{#                                                                            #}
{# Coefficients are derived from a principal-components rotation of cricket   #}
{# performance metrics. Higher score = better player. The original formula    #}
{# rotates "lower is better" bowling metrics (avg, economy, strike rate) so   #}
{# their contribution is subtracted, matching the paper's negated-index       #}
{# formulation.                                                               #}
{#                                                                            #}
{# IMPORTANT — methodological note for interview discussions:                 #}
{#   These macros recompute the PCA *from the same features* a downstream    #}
{#   ML model would predict it from. Treating the PCA as a prediction target  #}
{#   alongside its constituent features produces target leakage. The mart     #}
{#   models therefore expose the PCA as a *descriptive ranking metric*, not   #}
{#   as a regression target. See docs/adr-pca-leakage.md.                     #}
{# ────────────────────────────────────────────────────────────────────────── #}

{% macro compute_pca_batsman(
        runs, average, strike_rate, fours, sixes, hundreds, fifties) %}
    -- PCA_batsman = 0.458·R + 0.398·BA + 0.325·SR + 0.406·4s + 0.417·6s + 0.432·(100s + 50s)
    -- All inputs assumed non-null at this stage (Silver schema guarantees runs not null;
    -- COALESCE the rest to 0 to keep partial-data players in the ranking with a fair penalty).
    (
        0.458 * COALESCE({{ runs }}, 0)
      + 0.398 * COALESCE({{ average }}, 0)
      + 0.325 * COALESCE({{ strike_rate }}, 0)
      + 0.406 * COALESCE({{ fours }}, 0)
      + 0.417 * COALESCE({{ sixes }}, 0)
      + 0.432 * (COALESCE({{ hundreds }}, 0) + COALESCE({{ fifties }}, 0))
    )
{% endmacro %}


{% macro compute_pca_bowler(
        wickets, bowling_average, economy_rate, bowling_strike_rate) %}
    -- PCA_bowler = 0.428·W − 0.591·BA − 0.383·ER − 0.566·SR
    -- Wickets reward the bowler; the other three are inverse metrics (lower = better)
    -- so they're subtracted. Composite is reported as-is — can go negative for poor
    -- bowlers, which is the correct behavior.
    (
        0.428 * COALESCE({{ wickets }}, 0)
      - 0.591 * COALESCE({{ bowling_average }}, 0)
      - 0.383 * COALESCE({{ economy_rate }}, 0)
      - 0.566 * COALESCE({{ bowling_strike_rate }}, 0)
    )
{% endmacro %}
