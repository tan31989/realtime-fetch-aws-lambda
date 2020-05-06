package org.realtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Handler for requests to Lambda function.
 */
public class RealTime implements RequestHandler<Object, Object> {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public Object handleRequest(final Object input, final Context context) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        List<Employee> employees = new ArrayList<>();
        String url = "jdbc:mysql://host.docker.internal:3306/employees";
        String user = "root";
        String password = "";

        // Input JSON has all the data related to parameters being passed
        String inputValueStr = gson.toJson(input);
        // System.out.println("Input: " + inputValueStr);
        InputData inputData = gson.fromJson(inputValueStr, InputData.class);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, user, password);
            st = con.createStatement();
            String sqlQuery = "SELECT * FROM employee";
            if (inputData.pathParameters != null
                    && inputData.pathParameters.containsKey("employee_id")) {
                // WARNING: PLEASE do not do such type of query, this is subject to SQL INJECTION
                // ATTACK
                sqlQuery += " where id = " + inputData.pathParameters.get("employee_id");
            }
            sqlQuery += ";";
            System.out.println("SQL Query: " + gson.toJson(sqlQuery, String.class));
            rs = st.executeQuery(sqlQuery);

            while (rs.next()) {// get first result
                Employee employee = new Employee();
                employee.Id = rs.getString(1);
                employee.Name = rs.getString(2);
                employee.Salary = rs.getString(3);
                employees.add(employee);
            }
        } catch (SQLException | ClassNotFoundException ex) {
            Logger lgr = Logger.getLogger(RealTime.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(RealTime.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }

        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("X-Custom-Header", "application/json");
        String output = gson.toJson(employees);
        return new GatewayResponse(output, headers, 200);
    }
}
