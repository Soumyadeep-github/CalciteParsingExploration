<#assign list_cols_list = [] aggregations = [] abc = availableColumns?keys/>
<#list availableColumns?keys as column>
<#if availableColumns[column] != ""> <#assign col_name = "   `"+column+"`"+ " AS "+ availableColumns[column]/>
<#else> <#assign col_name = "   `"+column+"`"></#if>
<#assign list_cols_list = list_cols_list + [col_name] />
</#list>
<#list aggregationFunctions?keys as col>
<#assign aggregations = aggregations + [aggregationFunctions[col] +"("+col+")"] />
</#list>
<#assign list_of_aggs = ", " + aggregations?join(", \n")>
SELECT
${list_cols_list?join(", \n")}
<#if list_of_aggs != ""> ${list_of_aggs} </#if>
FROM ${tableName}
GROUP BY <#if list_of_aggs != "">${abc?join(", ")} </#if>;
