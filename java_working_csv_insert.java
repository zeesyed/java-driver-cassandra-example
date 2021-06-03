package com.cassandra.pilot;

import com.datastax.driver.core.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ValidateCluster {

    public static void main(String[] args) {
        Cluster validateCluster = null;

        String csvFile = "C:\\Users\\zsyed\\Downloads\\flights\\flights_from_pg_1.csv"; // Parse
        String line = ""; // Parse
        String cvsSplitBy = ","; // Parse
        String str2 = "2012/11/11";
        System.out.println(getdate1_fl_date(str2));
        try {
            validateCluster = validateCluster.builder() // (1)
                    .addContactPoint("10.2.2.22")
                    // .withPort(9160)
                    .build();
            Session session = validateCluster.connect("testflight01"); // (2)

            ResultSet rs = session.execute("select release_version from system.local"); // (3)
            Row row = rs.one();

            System.out.println(row.getString("release_version")); // (4)

            PreparedStatement statement = session.prepare(

                    "INSERT INTO flights"
                            + "(ID , YEAR ,DAY_OF_MONTH , FL_DATE , AIRLINE_ID , CARRIER , FL_NUM , ORIGIN_AIRPORT_ID , ORIGIN , ORIGIN_CITY_NAME , ORIGIN_STATE_ABR , DEST , DEST_CITY_NAME , DEST_STATE_ABR , DEP_TIME , ARR_TIME , ACTUAL_ELAPSED_TIME , AIR_TIME , DISTANCE)"
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

            // date1_fl_date
            // String sDate1 = "2012/11/11";
            // Date date1 = date1_fl_date(sDate1);
            // date3_dep_arr_date
            // String sDate3 = "1234";
            // Date date3 = date3_dep_arr_date(sDate3);
            // date5_actl_air_date
            // int sDate5 = 124;
            // Date date5 = date5_actl_air_date(sDate5);

            try {
                BufferedReader br = new BufferedReader(new FileReader(csvFile));

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] flights = line.split(cvsSplitBy);

                    // System.out.println("flights [CARRIER= " + flights[5] + " , FL_NUM=" +
                    // flights[6] + "]");

                    int ID = Integer.valueOf(flights[0]).intValue();
                    int YEAR = Integer.valueOf(flights[1]).intValue();
                    int DAY_OF_MONTH = Integer.valueOf(flights[2]).intValue();
                    int AIRLINE_ID = Integer.valueOf(flights[4]).intValue();
                    int FL_NUM = Integer.valueOf(flights[6]).intValue();
                    int ORIGIN_AIRPORT_ID = Integer.valueOf(flights[7]).intValue();
                    int DISTANCE = Integer.valueOf(flights[18]).intValue();
                    String CARRIER = flights[5];
                    String DEST = flights[11];
                    String DEST_CITY_NAME = flights[12];
                    String DEST_STATE_ABR = flights[13];
                    String ORIGIN = flights[8];
                    String ORIGIN_CITY_NAME = flights[9];
                    String ORIGIN_STATE_ABR = flights[10];
                    Date FL_DATE = date1_fl_date(flights[3]);
                    Date DEP_TIME = date3_dep_arr_date(flights[14], flights[3]);
                    Date ARR_TIME = date3_dep_arr_date(flights[15], flights[3]);
                    Date ACTUAL_ELAPSED_TIME = date5_actl_air_date(Integer.valueOf(flights[16]).intValue(), flights[3]);
                    Date AIR_TIME = date5_actl_air_date(Integer.valueOf(flights[17]).intValue(), flights[3]);

                    BoundStatement boundStatement = new BoundStatement(statement);
                    session.execute(boundStatement.bind(ID, YEAR, DAY_OF_MONTH, FL_DATE, AIRLINE_ID, CARRIER, FL_NUM,
                            ORIGIN_AIRPORT_ID, ORIGIN, ORIGIN_CITY_NAME, ORIGIN_STATE_ABR, DEST, DEST_CITY_NAME,
                            DEST_STATE_ABR, DEP_TIME, ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (validateCluster != null)
                validateCluster.close(); // (5)
        }
    }

    private static String minutestoHHmm(int t) {
        String hrs = null;
        String mins = null;
        try {
            int hours = t / 60; // since both are ints, you get an int
            int minutes = t % 60;
            hrs = String.format("%02d", hours);
            mins = String.format("%02d", minutes);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return String.valueOf(hrs) + String.valueOf(mins);
        }
    }

    private static Date getdate1_fl_date(String sDate1) {
        Date date1_fl_date = null;
        try {
            SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy/MM/dd");
            formatter1.setTimeZone(TimeZone.getTimeZone("UTC"));
            date1_fl_date = formatter1.parse(sDate1);

        } catch (ParseException e) {
            // e.printStackTrace();
            // continue;
        }
        return date1_fl_date;

    }

    private static Date date1_fl_date(String sDate1) {
        int excep1 = 0;
        SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy/MM/dd");
        Date date1_fl_date = null;
        try {
            formatter1.setTimeZone(TimeZone.getTimeZone("UTC"));
            date1_fl_date = formatter1.parse(sDate1);
        } catch (ParseException e) {
            // e.printStackTrace();
            excep1++;
        }
        // System.out.println("Total ");
        // System.out.println(date1_fl_date);
        return date1_fl_date;

    }

    private static Date date3_dep_arr_date(String sDate3, String sDate1) {
        SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/ddHHmm");
        Date date3_dep_arr_date = null;
        try {
            formatter3.setTimeZone(TimeZone.getTimeZone("UTC"));
            date3_dep_arr_date = formatter3.parse(sDate1 + sDate3);
        } catch (ParseException e) {
            // e.printStackTrace();
            int excep2 = 0;
            excep2++;
        }
        // System.out.println(date3_dep_arr_date);
        return date3_dep_arr_date;
    }

    private static Date date5_actl_air_date(int sDate5, String sDate1) throws ParseException {
        String sDateConvert4 = minutestoHHmm(sDate5);
        SimpleDateFormat formatter5 = new SimpleDateFormat("yyyy/MM/ddHHmm");
        formatter5.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date5_actl_air_date = formatter5.parse(sDate1 + sDateConvert4);
        // System.out.println(date5_actl_air_date);
        return date5_actl_air_date;
    }

}
