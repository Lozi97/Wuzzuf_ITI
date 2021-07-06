package jupai9.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.knowm.xchart.*;
import org.knowm.xchart.demo.charts.ExampleChart;
import org.knowm.xchart.demo.charts.pie.PieChart02;
import org.knowm.xchart.style.Styler;


import java.awt.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;
import java.util.List;
import java.text.ParseException; import java.util.ArrayList; import java.util.Collections; import java.util.Comparator; import java.util.HashMap; import java.util.LinkedHashMap; import java.util.List; import java.util.Map.Entry; import java.util.Set; import java.util.TreeMap;


public class Wuzzuf {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("Wuzzef App").master ("local[6]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
         Dataset<Row> wuzzefDF = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");

        // Print Schema to see column names, types and other metadata
        wuzzefDF.show (20);
        wuzzefDF.printSchema ();
        System.out.println ("Number of records: "+wuzzefDF.count ());

        //airbnbDF = airbnbDF.withColumn ("YearsExp", airbnbDF.col ("YearsExp").cast ("String")).filter (airbnbDF.col ("YearsExp") != "null Yrs of Exp");
        wuzzefDF = wuzzefDF.filter("YearsExp != 'null Yrs of Exp'");
        //airbnbDF.show(50);
        System.out.println ("Number of records Without Null records: "+wuzzefDF.count ());


        wuzzefDF = wuzzefDF.distinct();
        System.out.println ("Number of records Without Null records AND Duplicates: "+wuzzefDF.count ());

        wuzzefDF.show (20);






        /////////////////////////////////////// Factorize the YearsExp feature and convert it to numbers in new col //////////////////////////
        Dataset<Row> yearsExp = wuzzefDF.select("YearsExp");
        List<Row> rows = yearsExp.collectAsList();

        Integer a[] = new Integer[rows.size()];
        List min = Arrays.asList(a);

        Integer b[] = new Integer[rows.size()];
        List max = Arrays.asList(b);
        for(int j = 0; j < rows.size();j++) {
            min.set(j,0);
            max.set(j,0);
        }


        for(int i = 0; i < rows.size();i++)
        {
            if((rows.get(i).get(0).toString()).charAt(1) == '-')
            {
                //System.out.println((rows.get(i).get(0).toString()))
                min.set(i,Integer.parseInt(String.valueOf((rows.get(i).get(0).toString()).charAt(0))));
                if ((rows.get(i).get(0).toString()).charAt(3) == ' ')
                    max.set(i,Integer.parseInt(String.valueOf((rows.get(i).get(0).toString()).charAt(2))));
                else {
                   // System.out.println(String.valueOf(((rows.get(i).get(0).toString()).charAt(2)) + String.valueOf(((rows.get(i).get(0).toString()).charAt(3)))));
                    max.set(i, Integer.parseInt(String.valueOf(((rows.get(i).get(0).toString()).charAt(2)) + String.valueOf(((rows.get(i).get(0).toString()).charAt(3))))));
                }
            }
           else if ((rows.get(i).get(0).toString()).charAt(1) == '+')
            {
                min.set(i,Integer.parseInt(String.valueOf((rows.get(i).get(0).toString()).charAt(0))));
                max.set(i,40);
            }

           else if ((rows.get(i).get(0).toString()).charAt(2) == '-')
            {
                min.set(i,Integer.parseInt(String.valueOf(((rows.get(i).get(0).toString()).charAt(0)) + String.valueOf(((rows.get(i).get(0).toString()).charAt(1))))));
                if ((rows.get(i).get(0).toString()).charAt(4) == ' ')
                    max.set(i,Integer.parseInt(String.valueOf((rows.get(i).get(0).toString()).charAt(3))));
                else {
                    //System.out.println(String.valueOf(((rows.get(i).get(0).toString()).charAt(3)) + String.valueOf(((rows.get(i).get(0).toString()).charAt(4)))));
                    max.set(i, Integer.parseInt(String.valueOf(((rows.get(i).get(0).toString()).charAt(3)) + String.valueOf(((rows.get(i).get(0).toString()).charAt(4))))));
                }            }
           else if ((rows.get(i).get(0).toString()).charAt(2) == '+')
            {
               // System.out.println(((rows.get(i).get(0).toString()).charAt(0)));
               // System.out.println(((rows.get(i).get(0).toString()).charAt(1)));
               // System.out.println(String.valueOf(((rows.get(i).get(0).toString()).charAt(0)) +String.valueOf( ((rows.get(i).get(0).toString()).charAt(1)))));

                min.set(i,Integer.parseInt(String.valueOf(((rows.get(i).get(0).toString()).charAt(0)) +String.valueOf( ((rows.get(i).get(0).toString()).charAt(1))))));
                max.set(i,40);
            }

        }

//        for(int i = 0; i < rows.size();i++)
//        {
//        }
        //Dataset<Row> newDs = wuzzefDF.withColumn("Min",functions.array((Column) min));

        //newDs.show (10);

        //System.out.println((rows.get(i).get(0).toString()).charAt(1));
        System.out.println("Min Years of Experience");
        System.out.println(min);
        System.out.println("Max Years of Experience");
        System.out.println(max);


       // wuzzefDF = wuzzefDF.withColumn("new_col", (Column) min);

      //  wuzzefDF.select("Title", String.valueOf(min.stream().toArray( String[]::new))).show (10);
///////////////////////////////////////////////  get Top columns  /////////////////////////////////////////////////////////////////////////////
        //airbnbDF.groupBy("Company").sum().show(30);
        Dataset<Row> topCompanies = wuzzefDF.groupBy("Company").count().filter("count >= 10");
        Dataset<Row> topTenCompanies = topCompanies.sort(topCompanies.col("count").desc());
        topTenCompanies.show(10);



        Dataset<Row> topTitles = wuzzefDF.groupBy("Title").count().filter("count >= 10");
        Dataset<Row> topTenTitles = topTitles.sort(topTitles.col("count").desc());
        topTenTitles.show(10);

        //PieChart topTenTitlesChart = getChart(topTenTitles.collectAsList().subList(0,20),"Top 20 Titles");
        //new SwingWrapper<PieChart>(topTenTitlesChart).displayChart();


        Dataset<Row> topLocations = wuzzefDF.groupBy("Location").count().filter("count >= 10");
        Dataset<Row> topTenLocations = topLocations.sort(topLocations.col("count").desc());
        topTenLocations.show(10);
////////////////////////////////////////// Skills //////////////////////////////////////////////////////////////////////
        Dataset<Row> skills = wuzzefDF.select("Skills");
        List<Row> skillsRows = skills.collectAsList();

        String c[] = new String[skillsRows.size()];
        List ss = Arrays.asList(c);

        ArrayList<String> skills_list = new ArrayList<String>(); // Create an ArrayList object

        for(int i = 0; i < skillsRows.size();i++) {
            String s = (String) skillsRows.get(i).get(0);
            String[] res = s.split("[,]", 0);
            for(String myStr: res) {
                // System.out.println(myStr);

                skills_list.add(myStr);
            }
            //skills_list.add();
            //max.set(j,0);
        }
        System.out.println("Number of Skills with duplicates");

        System.out.println(skills_list.size());
        ArrayList<String> skills_list_without_duplicates = removeDuplicates(skills_list);
        System.out.println("Number of Skills without duplicates");

        System.out.println(skills_list_without_duplicates.size());
        System.out.println(" ");

        countFrequencies(skills_list);


////////////////////////////////////////// Charts ///////////////////////////////////////////////////////////////////////

        PieChart topTenCompaniesChart = getChart(topTenCompanies.collectAsList().subList(0,20),"Top 20 Companies");
        new SwingWrapper<PieChart>(topTenCompaniesChart).displayChart();


        CategoryChart topTenTitlesChart = getChart2(topTenTitles.collectAsList().subList(0,5),"Top 20 Titles");
        new SwingWrapper<CategoryChart>(topTenTitlesChart).displayChart();


        CategoryChart topTenLocationsChart = getChart2(topTenLocations.collectAsList().subList(0,8),"Top 20 Locations");
        new SwingWrapper<CategoryChart>(topTenLocationsChart).displayChart();




    }

    public static PieChart getChart(List<Row> rows, String name) {

        // Create Chart
        PieChart chart = new PieChartBuilder().width(800).height(600).title(name).build();

        // Customize Chart
        //Color[] sliceColors = new Color[] { new Color(224, 68, 14), new Color(230, 105, 62), new Color(236, 143, 110), new Color(243, 180, 159), new Color(246, 199, 182) };
        //chart.getStyler().setSeriesColors(sliceColors);

        for (int i = 0;i< rows.size();i++)
        {
            chart.addSeries((String)rows.get(i).get(0),(Long)rows.get(i).get(1));
        }


        return chart;
    }
    public static CategoryChart getChart2(List<Row> rows, String name) {

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(name).xAxisTitle("Name").yAxisTitle("Times").build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        String a[] = new String[rows.size()];
        List x = Arrays.asList(a);

        Long b[] = new Long[rows.size()];
        List y = Arrays.asList(b);

        // Series
        //chart.addSeries("test 1", Arrays.asList(new Integer[] { 0, 1, 2, 3, 4 }), Arrays.asList(new Integer[] { 4, 5, 9, 6, 5 }));
        for (int i = 0;i< rows.size();i++)
        {
            x.set(i,rows.get(i).get(0));
            y.set(i,rows.get(i).get(1));

        }

        chart.addSeries("Count", x, y);

        return chart;
    }

    public static <T> ArrayList<T> removeDuplicates(ArrayList<T> list)
    {

        // Create a new ArrayList
        ArrayList<T> newList = new ArrayList<T>();

        // Traverse through the first list
        for (T element : list) {

            // If this element is not present in newList
            // then add it
            if (!newList.contains(element)) {

                newList.add(element);
            }
        }

        // return the new list
        return newList;
    }

    public static void countFrequencies(ArrayList<String> list)
    {
        // hashmap to store the frequency of element
        Map<String, Integer> hm = new HashMap<String, Integer>();

        for (String i : list) {
            Integer j = hm.get(i);
            hm.put(i, (j == null) ? 1 : j + 1);
        }

//        TreeMap<String, Integer> sorted = new TreeMap(hm);
//        Set<Entry<String, Integer>> mappings = sorted.entrySet();
//        System.out.println("HashMap after sorting by keys in ascending order ");
//        for(Entry<String, Integer> mapping : mappings){ System.out.println(mapping.getKey() + " ==> " + mapping.getValue()); }
        Set<Entry<String, Integer>> entries = hm.entrySet();

        Comparator<Entry<String, Integer>> valueComparator = new Comparator<Entry<String,Integer>>()
        { @Override public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) { int v1 = e1.getValue();
            int v2 = e2.getValue(); return Integer.compare(v2, v1); } };
        String s = "";
        s.compareTo(s);
        // Sort method needs a List, so let's first convert Set to List in Java
        List<Entry<String, Integer>> listOfEntries = new ArrayList<Entry<String, Integer>>(entries);
        // sorting HashMap by values using comparator
         Collections.sort(listOfEntries, valueComparator);
         LinkedHashMap<String, Integer> sortedByValue = new LinkedHashMap<String, Integer>(listOfEntries.size());
         // copying entries from List to Map
        for(Entry<String, Integer> entry : listOfEntries){ sortedByValue.put(entry.getKey(), entry.getValue()); }
        System.out.println("Most important skills required:");
        Set<Entry<String, Integer>> entrySetSortedByValue = sortedByValue.entrySet();
        for(Entry<String, Integer> mapping : entrySetSortedByValue){ System.out.println(mapping.getKey() + " ==> " + mapping.getValue()); }



//        System.out.println(hm.values());
//        // displaying the occurrence of elements in the arraylist
//        int x= 0;
//        for (Map.Entry<String, Integer> val : hm.entrySet()) {
//           // System.out.println("Element " + x + " " + val.getKey() + " "
//            //        + "occurs"
//             //       + ": " + val.getValue() + " times");
//            x++;
//
//        }
    }
}
