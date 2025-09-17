# MongoDB to SQL Server Data Migration Tool

This project is a **Python-based utility** that transfers data from MongoDB collections into SQL Server tables.  
It is designed to automatically infer schemas, create corresponding tables in SQL Server, and insert all data with minimal manual intervention.  

---

## ✨ Key Features

- **Automatic Schema Inference**  
  Scans documents from MongoDB and determines the best matching SQL Server data types (int, bigint, decimal, float, datetime, char, nvarchar, json, etc.).  
  Handles MongoDB-specific types (ObjectId → NVARCHAR, Decimal128 → DECIMAL).  

- **Table Creation & Re-Creation**  
  Creates SQL Server table if it doesn’t exist.  
  Optionally drops and recreates the table if `ReCreateIfExists = 1`.  
  Ensures table name matches the MongoDB collection name.  

- **Multi-Collection Support**  
  Accepts one or more collection names (semicolon-separated).  
  Loops through each collection and processes them individually.  

- **Batch Data Insertion**  
  Inserts MongoDB documents into SQL Server in configurable batches.  
  Converts nested objects/lists into JSON strings for storage.  
  Handles errors gracefully and retries with safe conversions.  

- **Customizable**  
  - MongoDB connection (URI, DB, collections).  
  - SQL Server connection (Windows or SQL authentication).  
  - Sampling size for schema inference.  
  - Decimal precision/scale.  
  - Batch size for inserts.  

---

## ⚙️ How It Works

1. Connects to MongoDB and SQL Server.  
2. Scans MongoDB documents to infer schema.  
3. Generates a `CREATE TABLE` statement in SQL Server based on the inferred schema.  
4. Creates or recreates the table depending on `ReCreateIfExists`.  
5. Iterates through MongoDB documents, converts values into SQL-compatible types, and inserts into SQL Server.  
6. Commits data in batches for efficiency.  
7. Repeats the process for each collection provided.  

---

## 📊 Example Workflow

**MongoDB collection (Name: TestCollection):**

```json
{
  "_id": ObjectId("650c1b23d7"),
  "siteId": 12345,
  "status": "Active",
  "timestamp": ISODate("2025-09-17T08:30:00Z"),
  "details": { "alarm": "Overheat", "value": 90.5 }
}
```

**Inferred SQL Server table:**

```SQL
CREATE TABLE [dbo].[TestCollection] (
  [_id] NVARCHAR(24) NULL,
  [siteId] BIGINT NULL,
  [status] NVARCHAR(255) NULL,
  [timestamp] DATETIME2 NULL,
  [details] NVARCHAR(MAX) NULL
);
```
