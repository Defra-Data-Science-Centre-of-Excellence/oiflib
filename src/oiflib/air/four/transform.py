df_a4_bounds = df_a4_raw.assign(
    Lower_CI_bound=df_a4_raw[
        "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"
    ]
    - df_a4_raw["95% confidence interval for 'All sites' (+/-)"],
    Upper_CI_bound=df_a4_raw[
        "Annual average maximum daily 8-hour mean O3 concentration (µg/m3)"
    ]
    + df_a4_raw["95% confidence interval for 'All sites' (+/-)"],
)
