package inprogress.ApacheVelocity.dcrTemplates;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.context.Context;

import java.io.StringWriter;
import java.io.Writer;
import java.util.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RenderTemplates {

//    private static final Logger LOGGER = LoggerFactory.getLogger(RenderTemplates.class);
    private static Map<String, String> templatePlaceHolders;
    
    private static String cleanroomParticipants = Stream.of("WM", "FP")
            .collect(Collectors.joining("_"));
    
    private static Integer cleanroomId = 1;

    private static String collaborationEntity = cleanroomParticipants + "_" + cleanroomId;
    private static String ownerRoleName = "owner_" + collaborationEntity + "_role";
    private static String warehouseName = "owner_" + collaborationEntity + "_warehouse";

    private static String partnerRoleName = "partner_" + collaborationEntity + "_role";

    public static String parseTemplate(String templateName, Map<String, String> placeholders) {

        // TODO : take this from classpath
        Velocity.init("./src/main/java/inprogress/ApacheVelocity/velocity.properties");
        Template template = Velocity.getTemplate(String.format("/SnowFlake/%s", templateName));
        Context context = new VelocityContext();
        placeholders.forEach(context::put);
        Writer writer = new StringWriter();
        template.merge(context, writer);
        return writer.toString();
    }

    public static String createCollaborationQueries() {
        /*
        * TODO:
        *  Standardize nomenclature of a clean-room: <OWNER-PARTY>_<1st-PARTNER>_<2nd-PARTNER>_...._<CLEANROOM-ID-AUTOGENERATED>
        *  Standardize nomenclature of a role: <WALMART-AS-OWNER/PARTNER>_<<CLEANROOM-NAME>>
        * The reason why this choice was made was because unique role names only make sense when used at Walmart's account.
        * The advantage of having unique role names is avoiding collisions within the same account, as the current design
        * is built around
        * */
        String templateName = "1.CreateCollaboration.vm";
        List<String> dbs_objs = Arrays.asList("WM_Source_DB_O.WM_Source_SCHEMA_O",
                "WM_Source_DB_O.WM_Source_other_SCHEMA_O");
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("owner_role", ownerRoleName);
        templatePlaceHolders.put("db_schemas_tables", dbs_objs.stream()
                .collect(Collectors.joining(",")));
        return parseTemplate(templateName, templatePlaceHolders);
    }


    public static String invitePartnerQueries() {
        String templateName = "2.InvitePartner.vm";
        templatePlaceHolders = new HashMap<>();
        List<String> dbs_objs = Arrays.asList("FP_Source_DB_P.FP_Source_SCHEMA",
                "FP_Source_DB_P.FP_Source_other_SCHEMA");
        templatePlaceHolders.put("partner_cleanroom_db_name", "FP_DB");
        templatePlaceHolders.put("partner_cleanroom_share_name", "WM_FP_SHARE");
        templatePlaceHolders.put("partner_cleanroom_schema_name", "WM_FP_DB_SCHEMA");
        templatePlaceHolders.put("partner_role", partnerRoleName);
        templatePlaceHolders.put("owner_account_id", "CS98409"); // OY43846
        templatePlaceHolders.put("db_schemas_tables", null);
//                dbs_objs.stream()
//                .collect(Collectors.joining(",")));
        return parseTemplate(templateName, templatePlaceHolders);
    }


    public static String acceptInviteQueries() {
        String templateName = "3.AcceptInvite.vm";
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("owner_role", ownerRoleName);
        templatePlaceHolders.put("incoming_partner_cleanroom_db_name", "DB_FROM_FP_SHARE");
        templatePlaceHolders.put("partner_account_id", "OY43846");
        templatePlaceHolders.put("incoming_partner_cleanroom_share_name", "WM_FP_SHARE");
        templatePlaceHolders.put("customWarehouse", warehouseName);
        return parseTemplate(templateName, templatePlaceHolders);
    }


    /* TODO : ADD tables on partner side
    * */
    public static String onboardDatasetQueries(OnboardDatasetDTO onboardDatasetDto) {
        String templateName;
        if (onboardDatasetDto.getIsOwner()) {
            templateName = "4.OnboardDatasetOwner.vm";
            templatePlaceHolders = new HashMap<>();
            templatePlaceHolders.put("role_name", ownerRoleName);
            templatePlaceHolders.put("customWarehouse", warehouseName);
            templatePlaceHolders.put("destination_db_name", "WM_FP_CR_1_DB_O");
            templatePlaceHolders.put("destination_schema_name", "WM_FP_CR_1_SCHEMA_O");
            templatePlaceHolders.put("secure_view_name", "WM_Customers_VW");
            templatePlaceHolders.put("source_db_name", "WM_Source_DB_O");
            templatePlaceHolders.put("source_schema_name", "WM_Source_SCHEMA_O");
            templatePlaceHolders.put("source_table_name", "WM_Customer_SRC_TBL");
        } else {
            templateName = "5.OnboardDatasetPartner.vm";
            templatePlaceHolders = new HashMap<>();
            templatePlaceHolders.put("role_name", partnerRoleName);
//            templatePlaceHolders.put("customWarehouse", "compute_wh");
            templatePlaceHolders.put("destination_db_name", "FP_DB");
            templatePlaceHolders.put("destination_schema_name", "WM_FP_DB_SCHEMA");
            templatePlaceHolders.put("secure_view_name", "FP_Customers_VW");
            templatePlaceHolders.put("source_db_name", "FP_Source_DB_P");
            templatePlaceHolders.put("source_schema_name", "FP_Source_SCHEMA");
            templatePlaceHolders.put("source_table_name", "FP_Customer_SRC_TBL");
            templatePlaceHolders.put("partner_cleanroom_share_name", "WM_FP_SHARE");
            templatePlaceHolders.put("partner_role", "partner_WM_FP_1_role");
        }
        return parseTemplate(templateName, templatePlaceHolders);
    }

    /*
    public List<String> validateOnBoardPartnerQueries(OnboardDatasetDto onboardDatasetDto) {
        String templateName = "ValidateOnboardDataset.vm";
        templatePlaceHolders = getTemplatePlaceHolderValidateOnBoardDataset(onboardDatasetDto);
        return parseTemplate(templateName, templatePlaceHolders);
    }


    public String readDatasetSchema(ReadDatasetDto readDatasetDto) {
        String templateName = "ReadPartyDatasetSchema.vm";
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("source_db", readDatasetDto.getSourceDbName());
        templatePlaceHolders.put("source_schema", readDatasetDto.getSourceSchemaName());
        templatePlaceHolders.put("source_tbl", readDatasetDto.getSourceTableName());
        // Return the first element
        return parseTemplate(templateName, templatePlaceHolders).get(0);
    }


    public Integer executeQueries(String query, String party) throws SQLException, CollaborationException {
        Connection connection;
        if (party.equalsIgnoreCase("owner")) connection = ownerConnection;
        else connection = partnerConnection;
        try {
            Statement statement = connection.createStatement();
            LOGGER.info("Running: " + query);
            ResultSet result = statement.executeQuery(query);
            while (result.next())
                resultSetSize++;
            statement.close();
            System.out.println(connection);
            return resultSetSize;
        } catch (SQLException ex) {
            throw new SQLException(ex.getMessage());
        } catch (Exception ex) {
            throw new CollaborationException(ex.getMessage());
        }
    }

    // TODO: This is bad practice. remove connection parsing from string from here


    private Map<String, String> getTemplatePlaceHolderInvitePartner(InvitePartnerDto invitePartnerDto) {
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("partner_cleanroom_share_name", getPartnerShareName(invitePartnerDto.getCleanRoomName(), invitePartnerDto.getPartnerOrgName()));
        templatePlaceHolders.put("partner_cleanroom_db_name", getCleanroomPartnerDbName(invitePartnerDto.getPartnerOrgName()));
        templatePlaceHolders.put("partner_cleanroom_schema_name", getPartnerCleanRoomSchemaName(invitePartnerDto.getCleanRoomName()));
        templatePlaceHolders.put("owner_account_id", ownerId);
        return templatePlaceHolders;
    }

    private Map<String, String> getTemplatePlaceHolderAcceptInvite(InvitePartnerDto invitePartnerDto) {
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("incoming_partner_cleanroom_share_name", getPartnerShareName(invitePartnerDto.getCleanRoomName(), invitePartnerDto.getPartnerOrgName()));
        templatePlaceHolders.put("incoming_partner_cleanroom_db_name", getMountedDBName(invitePartnerDto.getCleanRoomName(), invitePartnerDto.getPartnerOrgName()));
        templatePlaceHolders.put("partner_account_id", partnerId);
        return templatePlaceHolders;
    }

    private Map<String, String> getTemplatePlaceHolderOnboardDatasetOwner(OnboardDatasetDto onboardDatasetDto) {
        templatePlaceHolders = new HashMap<>();
        //TODO: FILL
        templatePlaceHolders.put("source_schema_name", onboardDatasetDto.getSourceSchemaName());
        templatePlaceHolders.put("source_db_name", onboardDatasetDto.getSourceDbName());
        templatePlaceHolders.put("source_table_name", onboardDatasetDto.getSourceTableName());

        templatePlaceHolders.put("destination_schema_name", getOwnerCleanRoomSchemaName(onboardDatasetDto.getCleanRoomName()));
        templatePlaceHolders.put("destination_db_name", onboardDatasetDto.getSourceDbName());
        templatePlaceHolders.put("secure_view_name", getSecureViewName(onboardDatasetDto.getSourceTableName()));
        return templatePlaceHolders;
    }

    private Map<String, String> getTemplatePlaceHolderOnboardDatasetPartner(OnboardDatasetDto onboardDatasetDto) {
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("partner_cleanroom_share_name", getPartnerShareName(onboardDatasetDto.getCleanRoomName(), onboardDatasetDto.getOrgName()));
        templatePlaceHolders.put("source_schema_name", onboardDatasetDto.getSourceSchemaName());
        templatePlaceHolders.put("source_db_name", onboardDatasetDto.getSourceDbName());
        templatePlaceHolders.put("source_table_name", onboardDatasetDto.getSourceTableName());

        templatePlaceHolders.put("destination_schema_name", getPartnerCleanRoomSchemaName(onboardDatasetDto.getCleanRoomName()));
        templatePlaceHolders.put("destination_db_name", getCleanroomPartnerDbName(onboardDatasetDto.getOrgName()));
        templatePlaceHolders.put("secure_view_name", getSecureViewName(onboardDatasetDto.getSourceTableName()));
        return templatePlaceHolders;
    }

    private Map<String, String> getTemplateCreateCollaboration(CreateCollaborationDto createCollaborationDto) {
        return new HashMap<>();
    }

    private Map<String, String> getTemplatePlaceHolderValidateOnBoardDataset(OnboardDatasetDto onboardDatasetDto) {
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("incoming_partner_cleanroom_db_name", getMountedDBName(onboardDatasetDto.getCleanRoomName(), onboardDatasetDto.getOrgName()));
        templatePlaceHolders.put("partner_cleanroom_schema_name", getPartnerCleanRoomSchemaName(onboardDatasetDto.getCleanRoomName()));
        templatePlaceHolders.put("secure_view_name", getSecureViewName(onboardDatasetDto.getSourceTableName()));
        return templatePlaceHolders;
    }

     */

    public static String questionInstanceExecution() {
        String templateName = "QuestionInstanceExecution.vm";
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("role_name", ownerRoleName);
        templatePlaceHolders.put("customWarehouse", warehouseName);
        templatePlaceHolders.put("workflow_db", "JOBS");
        templatePlaceHolders.put("workflow_schema", "ANALYTICAL");
        templatePlaceHolders.put("job_view", "Q1_R1_vw");
        templatePlaceHolders.put("question_instance", "SELECT (100/1000)*50 AS calculation");
        return parseTemplate(templateName, templatePlaceHolders);
    }

    public static String testDollar() {
        String templateName = "test.vm";
        templatePlaceHolders = new HashMap<>();
        templatePlaceHolders.put("num", "20");
        templatePlaceHolders.put("wait", "----");
        return parseTemplate(templateName, templatePlaceHolders);
    }



    public static void main(String[] args) {
        System.out.print(testDollar());
//        System.out.println("-- STEP 1");
//        System.out.println("/************* CREATE COLLABORATION *****************/");
//        System.out.println(createCollaborationQueries());
//        System.out.println("-- STEP 2");
//        System.out.println("/************* INVITE PARTNER *****************/");
//        System.out.println(invitePartnerQueries()+"\n");
//        System.out.println("-- STEP 3");
//        System.out.println("/************* ACCEPT INVITE *****************/");
//        System.out.println(acceptInviteQueries());
//        System.out.println("-- STEP 4");
//        System.out.println("/************* ONBOARD OWNER DATASETS *****************/");
//        OnboardDatasetDTO onboardDatasetDtoT = new OnboardDatasetDTO(true);
//        System.out.println(onboardDatasetQueries(onboardDatasetDtoT));
//        System.out.println("-- STEP 5");
//        System.out.println("/************* ONBOARD PARTNER DATASETS *****************/");
//        OnboardDatasetDTO onboardDatasetDtoF = new OnboardDatasetDTO(false);
//        System.out.println(onboardDatasetQueries(onboardDatasetDtoF));
//        System.out.println("-- STEP 7");
//        System.out.println("/************* Question Instance Execution *****************/");
//        System.out.println(questionInstanceExecution());
    }

}
