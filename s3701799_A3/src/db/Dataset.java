package db;

import java.sql.SQLException;
import java.util.ArrayList;

import rmit.Settings;
import spatialindex.spatialindex.Point;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Dataset {
	// JDBC driver name and database URL
    private static String JDBC_DRIVER = "com.mysql.jdbc.Driver"; 
    private static String DB_URL = "jdbc:mysql://localhost/"+Settings.db_name;

    //  Database credentials
    private String USER;
    private String PASS;
   
    /* Creating local database connection object */
    public Dataset(String username, String password)
    {
        this.USER = username;
        this.PASS = password;        
    }
   
    public Connection Connect() throws SQLException{

        try{
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException exception){
            System.out.println("Database Driver Class Not found Exception: " + exception.toString());
            return null;
        }

        // Set connection timeout. Make sure you set this correctly as per your need
        DriverManager.setLoginTimeout(5);
        System.out.println("JDBC Driver Successfully Registered ...");

        try{
            System.out.println("Connecting Database ...");
            Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
            return conn;
        } catch (SQLException e){
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return null;
        }
    }
    
    public ArrayList<Point> loadTrajectoryPoints(Connection conn, String id) throws SQLException{
        
    	String result = "";
    	String sql = "SELECT distinct latitude, longitude from tb_la_dataset where trip_id = "+id;
        //System.out.println(sql);
        PreparedStatement st = conn.prepareStatement(sql);
        ArrayList<Point> pois = new ArrayList<>();
    	try{
    		ResultSet rs = st.executeQuery();
    		int c= 0;
    		while(rs.next()){
    			double[] f1 = new double[2];
    			f1[0] = Double.parseDouble(rs.getString("latitude")); f1[1] = Double.parseDouble(rs.getString("longitude"));
            	pois.add(new Point(f1));    			
            }
    		//System.out.println(c);
        }
        catch(SQLException e){
            e.printStackTrace();
            return null;
        }
        st.close();
        return pois;
        
    }    
   
    public String loadTrajectoryById(Connection conn, String id) throws SQLException{
        
    	String result = "";
    	String sql = "SELECT distinct latitude, longitude from tb_la_dataset where trip_id = "+id;
        //System.out.println(sql);
        PreparedStatement st = conn.prepareStatement(sql);
        try{
    		ResultSet rs = st.executeQuery();
    		int c= 0;
    		while(rs.next()){
            	if(result.equals("")){
            		result = rs.getString("latitude")+","+rs.getString("longitude");
            	}
            	else{
            		result += ","+rs.getString("latitude")+","+rs.getString("longitude");
            	}
            	c++;
            }
    		//System.out.println(c);
        }
        catch(SQLException e){
            e.printStackTrace();
            return null;
        }
        st.close();
        return result;
        
    }
    
    /* Closing database connection */
    public void DisConnect(Connection conn) throws SQLException{
        try{
            if(conn!=null){
                conn.close();
                System.out.println("Closing connection ...");
            }                            
        }
        catch(SQLException e){
            e.printStackTrace();
        }
    }
}
