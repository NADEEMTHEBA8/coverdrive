{# PCA composite for batsmen. Loadings from the 2022 PCA on standardized stats. #}
{% macro compute_pca_batsman(runs, batting_average, strike_rate, fours, sixes, hundreds, fifties) %}
    (
        0.458 * {{ runs }}
      + 0.398 * {{ batting_average }}
      + 0.325 * {{ strike_rate }}
      + 0.406 * {{ fours }}
      + 0.417 * {{ sixes }}
      + 0.432 * ({{ hundreds }} + {{ fifties }})
    )
{% endmacro %}

{% macro compute_pca_bowler(wickets, bowling_average, economy_rate, bowling_strike_rate) %}
    (
        0.428 * {{ wickets }}
      - 0.591 * {{ bowling_average }}
      - 0.383 * {{ economy_rate }}
      - 0.566 * {{ bowling_strike_rate }}
    )
{% endmacro %}
