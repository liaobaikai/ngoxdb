package com.liaobaikai.ngoxdb;


import net.ucanaccess.converters.SQLConverter;
import oracle.jdbc.OracleDatabaseMetaData;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author baikai.liao
 * @Time 2021-01-19 01:19:29
 */
public class Demo {

    // https://www.postgresql.org/docs/8.1/datatype.html
    static String[] dataTypes = {
            "bigint",
            "bigserial",
            "bit varying",
            "bit",
            "boolean",
            "box",
            "bytea",
            "character varying",
            "character",
            "cidr",
            "circle",
            "date",
            "double precision",
            "inet",
            "integer",
            "interval",
            "line",
            "lseg",
            "macaddr",
            "money",
            "numeric",
            "path",
            "point",
            "polygon",
            "real",
            "smallint",
            "serial",
            "text",
            "time",
            "timestamp"

    };

    /**
     * 脱去检查约束中的 CHECK ()
     * @param checkDefinition 检查约束定义
     * @return 约束定义
     */
    static String strip(String checkDefinition){

        // check()
        checkDefinition = checkDefinition.replaceFirst("CHECK[\\s\n\r]*\\(", "");
        checkDefinition = checkDefinition.replaceFirst("\\)$", "");

        // = ANY
        // https://www.postgresql.org/docs/9.6/functions-subquery.html#FUNCTIONS-SUBQUERY-ANY-SOME
        // SOME is a synonym for ANY. IN is equivalent to = ANY.
        checkDefinition = checkDefinition.replaceAll("= ANY", "IN").replaceAll("<> ALL", "NOT IN");

        // array[]
        checkDefinition = checkDefinition.replaceAll("\\(ARRAY\\[", "(").replaceAll("]", "");

        // ::dataType
        for(String dataType: dataTypes){
            checkDefinition = checkDefinition.replaceAll(String.format("::%s[\\[\\]]?", dataType), "");
        }



        // int leftParenthes = 0;  // (
        // int rightParenthes = 0; // )
        // int leftBracket = 0;    // [
        // int rightBracket = 0;   // ]
        // int singleQuotes = 0;   // '
        // int colon = 0;          // :
        //
        // StringBuilder sBuilder = new StringBuilder();
        // StringBuilder tmpBuilder = new StringBuilder();
        //
        // for(int i = 0, len = checkDefinition.length(); i < len; i++){
        //     char chr = checkDefinition.charAt(i);
        //     switch (chr){
        //         case '(':
        //             leftParenthes += 1;
        //             tmpBuilder.append(chr);
        //             break;
        //         case ')':
        //             rightParenthes += 1;
        //             break;
        //         case '[':
        //             leftBracket += 1;
        //             break;
        //         case ']':
        //             rightBracket += 1;
        //             break;
        //         case '\'':
        //             singleQuotes += 1;
        //             break;
        //         case ':':
        //             // 类型转换的符号
        //             // ::
        //             colon += 1;
        //             break;
        //         case ' ':
        //             // 空格将之前的字符删除
        //             sBuilder.append(chr).append(tmpBuilder);
        //             tmpBuilder.delete(0, tmpBuilder.length());
        //             break;
        //         default:
        //             // 其他字符
        //             if(singleQuotes % 2 == 0 && colon == 2){
        //                 // 忽略
        //                 break;
        //             }
        //             tmpBuilder.append(chr);
        //
        //             switch (tmpBuilder.toString()){
        //                 case "CHECK":
        //                     tmpBuilder.delete(0, tmpBuilder.length());
        //                     break;
        //                 case " ":
        //                     sBuilder.append(tmpBuilder);
        //                     tmpBuilder.delete(0, tmpBuilder.length());
        //                     break;
        //                 case "= ANY":
        //                     tmpBuilder.delete(0, tmpBuilder.length());
        //                     sBuilder.append("IN");
        //                     break;
        //                 case "ARRAY":
        //                     // 数组
        //                     break;
        //             }
        //     }
        // }


        return checkDefinition;
    }


    public static void main(String[] args) {

        // CHECK ((a > 0))
        // CHECK (((a)::text = 'baikai::character varying'::text))
        // CHECK (a > 10 AND (b::text = ANY (ARRAY['a'::character varying, 'b'::character varying, 'c'::character varying]::text[])))

        // ANY ( .* )
        // String s = "CHECK (a > 10 AND (b::text = ANY (ARRAY['a'::character varying, 'b'::character varying, 'c'''::character varying]::text[])) AND c > 0)";
        // // String s = "CHECK (((b)::text = ANY ((ARRAY['ab'::character varying, 'Ab'::character varying, 'aB'::character varying, 'AB'::character varying])::text[])))";
        //
        // s = strip(s);
        // System.out.println(s);

        String sql = "ALTER TABLE [film_actor] DROP CONSTRAINT [FILM_ACTOR_FK_FILM_ACTOR_ACTOR]";
        // String sql = "DISABLE AUTOINCREMENT ON [actor]";
        // String sql = "CREATE INDEX [idx_fk_address_id] ON [store]    (address_id ASC)";

        SQLConverter.DDLType ddlType = SQLConverter.getDDLType(sql);
        System.out.println(ddlType);



    }
}
