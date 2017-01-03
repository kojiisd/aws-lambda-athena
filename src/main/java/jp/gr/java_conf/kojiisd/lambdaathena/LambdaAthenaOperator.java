package jp.gr.java_conf.kojiisd.lambdaathena;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import jp.gr.java_conf.kojiisd.lambdaathena.dto.Request;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Accessing to operate Athena
 *
 * @author kojiisd
 */
public class LambdaAthenaOperator implements RequestHandler<Request, Object> {
    private Map<String, String> regionMap = new HashMap();

    public LambdaAthenaOperator() {
        this.regionMap.put("us-east-1", "us-east-1");
        this.regionMap.put("us-west-2", "us-west-2");
    }

    public Object handleRequest(Request input, Context context) {
        Connection conn = null;
        Statement statement = null;
        StringBuilder columnSb = null;

        LambdaLogger logger = context.getLogger();

        boolean valid = isRequiredValid(input);

        Formatter formatter = new Formatter();
        String athenaUrl = formatter.format("jdbc:awsathena://athena.%s.amazonaws.com:443", input.region).toString();
        logger.log("Access to :" + athenaUrl + "\n");

        if (!valid) {
            return "Input parameters are not enough. input: " + input;
        }

        try {
            Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");
            Properties info = new Properties();
            if (StringUtils.isBlank(input.s3Path)) {
                info.put("s3_staging_dir", "s3://");
            } else {
                info.put("s3_staging_dir", input.s3Path);
            }
            info.put("aws_credentials_provider_class", "com.amazonaws.auth.PropertiesFileCredentialsProvider");

            // Put credential information.
            info.put("aws_credentials_provider_arguments", "config/credential");

            conn = DriverManager.getConnection(athenaUrl, info);

            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(input.sql);

            String[] columnArray = input.columnListStr.split(",");
            columnSb = new StringBuilder();
            columnSb.append(input.columnListStr).append(System.getProperty("line.separator"));

            while (rs.next()) {
                int length = columnSb.length();

                //Retrieve table column.
                for (String column : columnArray) {
                    if (StringUtils.isBlank(column)) {
                        continue;
                    }
                    columnSb.append(",").append(rs.getString(column.trim()));
                }
                columnSb.delete(length, length + 1);
                columnSb.append(System.getProperty("line.separator"));
            }
            rs.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();

            return "Exception happened, aborted.";
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (Exception ex) {

            }
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception ex) {

                ex.printStackTrace();
            }
        }

        String result = "Input parameter:" + input.toString() + "\n\nresult:\n" + columnSb.toString();
        logger.log("Finished connecting to Athena.\n");

        logger.log(result);

        return result;
    }


    private boolean isRequiredValid(Request request) {
        if (request == null) {
            return false;
        }

        if (StringUtils.isBlank(request.s3Path)) {
            return false;
        }

        if (StringUtils.isBlank(request.sql)) {
            return false;
        }

        if (StringUtils.isBlank(request.columnListStr)) {
            return false;
        }

        return true;
    }

}
