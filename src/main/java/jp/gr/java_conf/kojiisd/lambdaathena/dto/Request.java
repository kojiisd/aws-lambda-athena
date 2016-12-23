package jp.gr.java_conf.kojiisd.lambdaathena.dto;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Lambda Request Data Class
 * @author kojiisd
 */
public class Request {

    @Getter
    @Setter
    public String region;

    @Getter
    @Setter
    public String s3Path;

    @Getter
    @Setter
    public String sql;

    @Getter
    @Setter
    public String columnListStr;

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
