<#assign list_cols = [] />
<#list availableCols?keys as col>
<#if availableCols[col] != ""><#assign col_name = "   `"+col+"`"+ " AS "+ availableCols[col]/>
<#else> <#assign col_name = "   `"+col+"`"></#if>
<#assign list_cols = list_cols + [col_name] />
</#list>
SELECT
${list_cols?join(", \n")}
FROM ${tableName};