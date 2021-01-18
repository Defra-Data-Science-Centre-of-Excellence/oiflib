df_a3_melted['year'] = df_a3_melted.variable.str.extract('(\d{4})')
df_a3_melted['measure'] = df_a3_melted.variable.str.extract('\((\S*)\)')

df_a3_eng_tot = (
    df_a3_melted
    .query('Country == "England" & measure == "total"')
)[['Country', 'year', 'ugm-3']]