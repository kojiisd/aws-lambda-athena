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
 * @author kojiisd
 */
public class LambdaAthenaOperator implements RequestHandler<Request, Object> {
    private Map<String, String> regionMap = new HashMap();

    public LambdaAthenaOperator() {
        this.regionMap.put("us-east-1", "us-east-1");
        this.regionMap.put("us-east-2", "us-east-2");
    }

    public Object handleRequest(Request input, Context context) {
        Connection conn = null;
        Statement statement = null;

        LambdaLogger logger = context.getLogger();

        boolean valid = isValid(input);

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
            String databaseName;
            if (StringUtils.isBlank(input.database)) {
                databaseName = "default";
            } else {
                databaseName = input.database;
            }

            conn = DriverManager.getConnection(athenaUrl, info);

            String sql = "show tables in " + databaseName;
            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);

            while (rs.next()) {
                //Retrieve table column.
                String name = rs.getString("tab_name");

                //Display values.
                logger.log("Name: " + name + "\n");
            }
            rs.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
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
        logger.log("Finished connecting to Athena.\n");

        return input.toString();
    }


    private boolean isValid(Request request) {
        if (StringUtils.isBlank(request.database)) {
            return false;
        }

        if (StringUtils.isBlank(request.s3Path)) {
            return false;
        }

        if (StringUtils.isBlank(request.region) || StringUtils.isBlank(this.regionMap.get(request.region))) {
            return false;
        }

        return true;
    }
}
