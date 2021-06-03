package com.cassandra.pilot;

import com.datastax.driver.core.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ValidateCluster {

    public static void main(String[] args) {
        Cluster validateCluster = null;

        String csvFile = "C:\\Users\\zsyed\\Downloads\\flights\\flights_from_pg.csv";                                   //Parse
        String line = "";                                                                                               //Parse
        String cvsSplitBy = ",";                                                                                        //Parse

        try {
            validateCluster = validateCluster.builder() // (1)
                    .addContactPoint("10.2.2.109")
                    //.withPort(9160)
                    .build();
            Session session = validateCluster.connect("testflight01"); // (2)

            ResultSet rs = session.execute("select release_version from system.local"); // (3)
            Row row = rs.one();

            System.out.println(row.getString("release_version")); // (4)

            //date1_fl_date
            String sDate1 = "2012/11/11";
            SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy/MM/dd");
            formatter1.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date1_fl_date = formatter1.parse(sDate1);
            //date3_dep_arr_date
            String sDate3 = "1234";
            SimpleDateFormat formatter3 = new SimpleDateFormat("yyyy/MM/ddHHmm");
            formatter3.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date3_dep_arr_date = formatter3.parse(sDate1 + sDate3);
            System.out.println(date3_dep_arr_date);
            //date5_actl_air_date
            int sDate5 = 124;
            String sDateConvert4 = minutestoHHmm(sDate5);
            SimpleDateFormat formatter5 = new SimpleDateFormat("yyyy/MM/ddHHmm");
            formatter5.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date date5_actl_air_date = formatter5.parse(sDate1 + sDateConvert4);

            try {
                BufferedReader br = new BufferedReader(new FileReader(csvFile));

                while ((line = br.readLine()) != null) {

                    // use comma as separator
                    String[] flights = line.split(cvsSplitBy);

                    //System.out.println("flights [CARRIER= " + flights[5] + " , FL_NUM=" + flights[6] + "]");

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

                    PreparedStatement statement = session.prepare(

                            "INSERT INTO flights" + "(ID , YEAR ,DAY_OF_MONTH , FL_DATE , AIRLINE_ID , CARRIER , FL_NUM , ORIGIN_AIRPORT_ID , ORIGIN , ORIGIN_CITY_NAME , ORIGIN_STATE_ABR , DEST , DEST_CITY_NAME , DEST_STATE_ABR , DEP_TIME , ARR_TIME , ACTUAL_ELAPSED_TIME , AIR_TIME , DISTANCE)" +
                                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

                    BoundStatement boundStatement = new BoundStatement(statement);
                    session.execute(boundStatement.bind(ID, YEAR, DAY_OF_MONTH, date1_fl_date, AIRLINE_ID, CARRIER, FL_NUM, ORIGIN_AIRPORT_ID, ORIGIN, ORIGIN_CITY_NAME, ORIGIN_STATE_ABR, DEST, DEST_CITY_NAME, DEST_STATE_ABR, new Date(), new Date(), new Date(), new Date(), DISTANCE));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (validateCluster != null) validateCluster.close(); // (5)
        }
    }

    private static String minutestoHHmm(int t) {
        int hours = t / 60; //since both are ints, you get an int
        int minutes = t % 60;
        String hrs = String.format("%02d", hours);
        String mins = String.format("%02d", minutes);
        return String.valueOf(hrs) + String.valueOf(mins);
    }
}