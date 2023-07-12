<#assign create_cols = []/>
<#list availableColumns?keys as col>
<#assign create_cols = create_cols + [col + " " + availableColumns[col]] />
</#list>
CREATE TABLE ${tableName} IF NOT EXISTS (
${create_cols?join(", \n")}
)
;