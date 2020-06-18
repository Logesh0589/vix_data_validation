package org.vix;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;

public class validateDataDQ implements Function<Row, Row> {
    static String checkRecordDesc = "";
    @Override
    public Row call(Row row) {
        String checkRecord = "true";
        checkRecordDesc = "";

        String parseVixDate = validateDate("VixDate", row.getString(vixDataConstants.vixDateIndex));
        String parseVixOpen = validateDouble("VixOpen", row.getString(vixDataConstants.vixOpenIndex));
        String parseVixHigh = validateDouble("VixHigh", row.getString(vixDataConstants.vixHighIndex));
        String parseVixLow = validateDouble("VixLow", row.getString(vixDataConstants.vixLowIndex));
        String parseVixClose = validateDouble("VixClose", row.getString(vixDataConstants.vixCloseIndex));

        if(checkRecordDesc.isEmpty()){
            double checkVixOpen = Double.parseDouble(parseVixOpen);
            double checkVixHigh = Double.parseDouble(parseVixHigh);
            double checkVixLow = Double.parseDouble(parseVixLow);
            double checkVixClose = Double.parseDouble(parseVixClose);

            if (checkVixHigh < checkVixLow) {
                checkRecordDesc="VixHigh is smaller than VixLow";
                checkRecord = "false";
            } else if (checkVixOpen > checkVixHigh || checkVixOpen < checkVixLow) {
                checkRecordDesc="VixOpen value is not between VixHigh and VixLow";
                checkRecord = "false";
            } else if (checkVixClose > checkVixHigh || checkVixClose < checkVixLow) {
                checkRecordDesc="VixClose value is not between VixHigh and VixLow";
                checkRecord = "false";
            }
        } else {
            checkRecord = "false";
        }
        return RowFactory.create(parseVixDate, parseVixOpen, parseVixHigh, parseVixLow, parseVixClose, checkRecord, checkRecordDesc);
    }

    private static String validateDate(String fieldName, String fieldVal)  {

        SimpleDateFormat givenFormat = new SimpleDateFormat("MM/dd/yy");
        SimpleDateFormat actualFormat = new SimpleDateFormat("yyyy-MM-dd");
        String formatVixDate;
        try {
            Date parseDate = givenFormat.parse(fieldVal);
            formatVixDate = actualFormat.format(parseDate);

            LocalDate now = LocalDate.now();
            Period findFutureDate = Period.between(parseDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate(), now);

           if (findFutureDate.isNegative()) {
                checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + "Future Date is found";
           }

        } catch(NullPointerException e) {
            checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + e;
            return fieldVal;
        } catch(java.text.ParseException e) {
            checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + e;
            return fieldVal;
        } catch (Exception e) {
            checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + e;
            return fieldVal;
        }
        return formatVixDate;
    }

    private static String validateDouble(String fieldName, String fieldVal)  {
        Double parseDouble;
        try {
            parseDouble = Double.parseDouble(fieldVal);
        } catch(NullPointerException e) {
            checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + e;
            return fieldVal;
        }
        catch(NumberFormatException e) {
            checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + e;
            return fieldVal;
        } catch (Exception e) {
            checkRecordDesc = checkRecordDesc + " " + fieldName + ": " + e;
            return fieldVal;
        }
        return Double.toString(parseDouble);
    }
}
