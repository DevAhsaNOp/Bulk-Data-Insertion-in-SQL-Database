using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace EmployeeInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = "Data Source=DEV-AHSAN\\SQLEXPRESS;Initial Catalog=EmployeeDb;Integrated Security=True";

            var EmployeeList = new List<Employee>()
            {
                new Employee { Firstname = "John", Lastname = "Doe", AddressId = 1008, Email = "John.Doe43@gmail.com"},
                new Employee { Firstname = "Sameer", Lastname = "Taha", AddressId = 2004, Email = "Sameer.Taha43@gmail.com"},
                new Employee { Firstname = "Ilyaas", Lastname = "Khan", AddressId = 2005, Email = "Ilyaas.Khan43@gmail.com"},
                new Employee { Firstname = "Omar", Lastname = "Ali", AddressId = 2006, Email = "Omar.Ali43@gmail.com"}
            };

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // Create a temporary table with additional ProcessDescription column.
                string createTempTableSql = @"
                    CREATE TABLE #TempEmployee (
                        Firstname VARCHAR(50),
                        Lastname VARCHAR(50),
                        AddressId INT,
                        Email VARCHAR(50),
                        ProcessDescription NVARCHAR(MAX)
                    )";

                using (SqlCommand createTableCommand = new SqlCommand(createTempTableSql, connection))
                {
                    createTableCommand.ExecuteNonQuery();
                }

                // Get column information from the Employee table.
                Dictionary<string, Tuple<string, int>> columnInfo = GetTableColumnsInfoGeneric(connection, "Employee");
                columnInfo.Remove("Id"); // Remove the Id column from the dictionary.

                // Insert records into the temporary table with validation.
                foreach (Employee employee in EmployeeList)
                {
                    string processDescription = ValidateSchemaForGenericClass(employee, columnInfo);

                    if (processDescription != "Validation passed")
                        processDescription += " " + ValidateIsEmployeeAddressExist(connection, employee);
                    else
                        processDescription = ValidateIsEmployeeAddressExist(connection, employee);

                    using (SqlCommand insertCommand = new SqlCommand(
                        "INSERT INTO #TempEmployee (Firstname, Lastname, AddressId, Email, ProcessDescription) VALUES (@Firstname, @Lastname, @AddressId, @Email, @ProcessDescription)",
                        connection))
                    {
                        insertCommand.Parameters.AddWithValue("@Firstname", employee.Firstname);
                        insertCommand.Parameters.AddWithValue("@Lastname", employee.Lastname);
                        insertCommand.Parameters.AddWithValue("@AddressId", employee.AddressId);
                        insertCommand.Parameters.AddWithValue("@Email", employee.Email);
                        insertCommand.Parameters.AddWithValue("@ProcessDescription", processDescription);
                        insertCommand.ExecuteNonQuery();
                    }
                }

                // Now, you can check the ProcessDescription column for error messages.
                DataTable errorDataTable = GetErrorDataTable(connection);
                DataTable dataTable = null;

                // If there are no validation errors, insert data into the Employee table.
                if (errorDataTable.Rows.Count == 0)
                {
                    try
                    {
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                        {
                            // Replace with your actual table name.
                            bulkCopy.DestinationTableName = "dbo.Employee";
                            dataTable = CreateDataTableForGenericClass(EmployeeList);

                            //[OPTIONAL]: Map the DataTable columns with that of the database table
                            Type itemType = typeof(Employee);
                            PropertyInfo[] properties = itemType.GetProperties();
                            foreach (PropertyInfo property in properties)
                            {
                                bulkCopy.ColumnMappings.Add(property.Name, property.Name);
                            }

                            #region Columns Mapping Manually

                            //bulkCopy.ColumnMappings.Add("Firstname", "Firstname");
                            //bulkCopy.ColumnMappings.Add("Lastname", "Lastname");
                            //bulkCopy.ColumnMappings.Add("AddressId", "AddressId");
                            //bulkCopy.ColumnMappings.Add("Email", "Email");

                            #endregion

                            bulkCopy.WriteToServer(dataTable);
                            Console.WriteLine("Bulk copy operation completed successfully.");
                        }
                    }
                    catch (Exception ex)
                    {
                        // Log the error message and the problematic data.
                        Console.WriteLine("Error Message: " + ex.Message);

                        if (ex is SqlException sqlException)
                        {
                            foreach (SqlError error in sqlException.Errors)
                            {
                                Console.WriteLine($"SqlError: {error.Number} - {error.Message}");
                            }
                        }
                    }
                }

                // Drop the temporary table when done.
                string dropTempTableSql = "DROP TABLE #TempEmployee";
                using (SqlCommand dropTableCommand = new SqlCommand(dropTempTableSql, connection))
                {
                    dropTableCommand.ExecuteNonQuery();
                }

                connection.Close();

                // Print error messages, if any.
                foreach (DataRow row in errorDataTable.Rows)
                {
                    Console.WriteLine(row["ProcessDescription"]);
                }
            }

            Console.ReadKey();
        }

        /// <summary>
        /// Generic method to give table columns information. 
        /// Pass SQL Connection Instance <seealso cref="SqlConnection"/>
        /// and Pass SQL Source Table Name <seealso cref="string"/>
        /// </summary>
        /// <param name="connection">Pass SQL Connection Instance</param>
        /// <param name="tableName">Pass SQL Source Table Name</param>
        /// <returns><seealso cref="Dictionary{TKey, TValue}"/> of table columns information</returns>
        private static Dictionary<string, Tuple<string, int>> GetTableColumnsInfoGeneric(SqlConnection connection, string tableName)
        {
            Dictionary<string, Tuple<string, int>> columnInfo = new Dictionary<string, Tuple<string, int>>();

            string getColumnInfoSql = $"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";
            using (SqlCommand command = new SqlCommand(getColumnInfoSql, connection))
            {
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string dataType = reader.GetString(1);
                        int maxLength = reader.IsDBNull(2) ? -1 : reader.GetInt32(2);

                        columnInfo[columnName] = new Tuple<string, int>(dataType, maxLength);
                    }
                }
            }

            return columnInfo;
        }

        /// <summary>
        /// Check if the current employee 'Address ID' is exist in the Table Address.
        /// Pass <seealso cref="SqlConnection"/> Instance
        /// and Pass <seealso cref="Employee"/> Object Instance
        /// </summary>
        /// <param name="connection">Pass SQL Connection Instance</param>
        /// <param name="employee">Pass Employee Object Instance</param>
        /// <returns>A <seealso cref="string"/> contains 'Validation Passed' or 'Failed'</returns>
        private static string ValidateIsEmployeeAddressExist(SqlConnection connection, Employee employee)
        {
            string processDescription = "Validation passed";

            // Data validation and AddressId check.
            using (SqlCommand validationCommand = new SqlCommand(
                "SELECT COUNT(*) FROM Address WHERE AddId = @AddressId",
                connection))
            {
                validationCommand.Parameters.AddWithValue("@AddressId", employee.AddressId);
                int addressCount = (int)validationCommand.ExecuteScalar();

                if (addressCount == 0)
                {
                    processDescription = "AddressId not found in Address table for the Employee email: " + employee.Email;
                }
            }

            return processDescription;
        }

        /// <summary>
        /// Validate the schema of the generic class.
        /// Pass your 'class' object Instance
        /// and Pass <seealso cref="Dictionary{TKey, TValue}"/> of Columns Information
        /// </summary>
        /// <param name="obj">Pass your 'class' object Instance</param>
        /// <param name="columnInfo">Pass <seealso cref="Dictionary{TKey, TValue}"/> of Columns Information</param>
        /// <returns>A <seealso cref="string"/> contains 'Validation Passed' or 'Failed'</returns>
        public static string ValidateSchemaForGenericClass<T>(T obj, Dictionary<string, Tuple<string, int>> columnInfo)
        {
            string processDescription = "Validation passed";

            Type objectType = typeof(T);
            PropertyInfo[] properties = objectType.GetProperties();

            foreach (var kvp in columnInfo)
            {
                string columnName = kvp.Key;
                string expectedDataType = kvp.Value.Item1;
                int maxLength = kvp.Value.Item2;

                PropertyInfo property = properties.FirstOrDefault(p => p.Name == columnName);

                if (property == null)
                {
                    processDescription = $"Column {columnName} not found in the object.";
                    break;
                }

                object propertyValue = property.GetValue(obj);
                string actualValue = (propertyValue != null) ? propertyValue.ToString() : null;

                // Check if the data type matches the expected data type.
                if (!IsValidDataType(actualValue, expectedDataType))
                {
                    var employeeEmail = properties.FirstOrDefault(p => p.Name == "Email").GetValue(obj);
                    processDescription = $"Invalid data type for {columnName} column. Expected data type: {expectedDataType} for Employee Email: {employeeEmail}";
                    break;
                }

                // Validate the length if applicable.
                if (maxLength != -1 && (actualValue == null || actualValue.Length > maxLength))
                {
                    var employeeEmail = properties.FirstOrDefault(p => p.Name == "Email").GetValue(obj);
                    processDescription = $"Data too long for {columnName} column. Maximum length allowed: {maxLength} for Employee Email: {employeeEmail}";
                    break;
                }
            }

            return processDescription;
        }

        /// <summary>
        /// Checks if the actual value of the class property matches the expected data type of the Database Column.
        /// Pass your actual value in <seealso cref="string"/>
        /// and Pass expected data type in <seealso cref="string"/> of Column
        /// </summary>
        /// <param name="expectedDataType">Pass expected data type in <seealso cref="string"/> of Column</param>
        /// <param name="value">Pass your actual value in <seealso cref="string"/></param>
        /// <returns>A <seealso cref="bool"/> contains true or false</returns>
        private static bool IsValidDataType(string value, string expectedDataType)
        {
            // Add logic to check if the actual data type matches the expected data type.
            // You can use type checking methods or patterns depending on your requirements.
            // Here's a simple example using type names for illustration:
            switch (expectedDataType)
            {
                case "varchar":
                    return value is string;
                case "int":
                    int intValue;
                    return int.TryParse(value, out intValue);
                // Add more data types as needed.
                default:
                    return false; // Unknown data type.
            }
        }

        /// <summary>
        /// Checks if the Temporary Table Column ProcessDescription contains 'Validation Passed' or 'Failed'
        /// Pass <seealso cref="SqlConnection"/> Instance
        /// </summary>
        /// <param name="connection">Pass <seealso cref="SqlConnection"/> Instance</param>
        /// <returns>A <seealso cref="DataTable"/> contains 'Validation Passed' or 'Failed' with all records failed entry</returns>
        private static DataTable GetErrorDataTable(SqlConnection connection)
        {
            DataTable errorDataTable = new DataTable();
            using (SqlDataAdapter adapter = new SqlDataAdapter("SELECT * FROM #TempEmployee WHERE ProcessDescription <> 'Validation passed'", connection))
            {
                adapter.Fill(errorDataTable);
            }
            return errorDataTable;
        }

        #region Create DataTable For Specific Class

        //private static DataTable CreateDataTableFromEmployeeList(List<Employee> employees)
        //{
        //    DataTable dataTable = new DataTable();
        //    dataTable.Columns.Add("Firstname", typeof(string));
        //    dataTable.Columns.Add("Lastname", typeof(string));
        //    dataTable.Columns.Add("AddressId", typeof(int));
        //    dataTable.Columns.Add("Email", typeof(string));

        //    // You can specify varchar for specific columns as needed.
        //    dataTable.Columns["Firstname"].ExtendedProperties.Add("SqlDbType", SqlDbType.VarChar);
        //    dataTable.Columns["Lastname"].ExtendedProperties.Add("SqlDbType", SqlDbType.VarChar);
        //    dataTable.Columns["Email"].ExtendedProperties.Add("SqlDbType", SqlDbType.VarChar);
        //    dataTable.Columns["AddressId"].ExtendedProperties.Add("SqlDbType", SqlDbType.Int);

        //    foreach (Employee employee in employees)
        //    {
        //        dataTable.Rows.Add( employee.Firstname, employee.Lastname, employee.AddressId, employee.Email);
        //    }

        //    return dataTable;
        //}

        #endregion

        /// <summary>
        /// Create DataTable For Generic 'Class' List
        /// Pass your 'Class' object List
        /// </summary>
        /// <param name="items">Pass your 'Class' object List</param>
        /// <returns>A <seealso cref="DataTable"/> contains all records</returns>
        private static DataTable CreateDataTableForGenericClass<T>(List<T> items)
        {
            DataTable dataTable = new DataTable();

            // Get the type of the generic class.
            Type itemType = typeof(T);

            // Get all the public properties of the class.
            PropertyInfo[] properties = itemType.GetProperties();

            foreach (PropertyInfo property in properties)
            {
                // Add a DataColumn for each property.
                dataTable.Columns.Add(property.Name, Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }

            foreach (T item in items)
            {
                DataRow row = dataTable.NewRow();

                foreach (PropertyInfo property in properties)
                {
                    // Set the value of each DataColumn to the corresponding property value of the object.
                    row[property.Name] = property.GetValue(item) ?? DBNull.Value;
                }

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }
    }

    class Employee
    {
        public string Firstname { get; set; }
        public string Lastname { get; set; }
        public int AddressId { get; set; }
        public string Email { get; set; }
    }
}
