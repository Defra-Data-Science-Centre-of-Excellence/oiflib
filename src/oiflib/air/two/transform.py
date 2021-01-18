

df_a2_raw.NCFormat = df_a2_raw.NCFormat.ffill()

df_a2_raw['category'] = None

df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Land use') & df_a2_raw.IPCC.str.contains('4[A|G]'), 'category'] = 'Forestry sink'
df_a2_raw.loc[df_a2_raw.NCFormat == 'Agriculture', 'category'] = 'Agriculture'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Business') & df_a2_raw.IPCC.str.contains('2[E-F]|2G[1-2]'), 'category'] = 'Fluorinated gases'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Industrial') & df_a2_raw.IPCC.str.contains('2B9|2C[3-4]'), 'category'] = 'Fluorinated gases'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Residential') & df_a2_raw.IPCC.str.contains('2F4'), 'category'] = 'Fluorinated gases'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Land use') & df_a2_raw.IPCC.str.contains('4[B-E|_]'), 'category'] = 'Land use & land use change'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Business') & df_a2_raw.IPCC.str.contains('5C'), 'category'] = 'Waste'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Residential') & df_a2_raw.IPCC.str.contains('5[B-C]'), 'category'] = 'Waste'
df_a2_raw.loc[df_a2_raw.NCFormat.str.contains('Waste') & df_a2_raw.IPCC.str.contains('5[A-D]'), 'category'] = 'Waste'

df_a2_melted = (
    df_a2_raw
    .groupby('category')
    .sum()
    .reset_index()
    .melt(id_vars='category', var_name='Year', value_name='GHG - kt CO2 Equiv')
    )