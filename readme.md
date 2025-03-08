# IgnisQL

IgnisQL is an AI-powered SQL correction and generation tool that leverages **Large Language Models (LLMs)** to analyze and fix SQL queries based on database schema information. The project is designed to assist users in correcting erroneous SQL queries and generating valid SQL statements based on natural language prompts.

## 🚀 Approach

### 1. **Schema Extraction**

- `schema_extractor.py` retrieves **table structures**, **column details**, **primary keys**, and **foreign key relationships** from the database.
- The extracted schema is formatted into a structured **text prompt** to provide context for the LLM.

### 2. **SQL Correction & Execution**

- `sql_corrector.py` identifies errors in SQL queries and provides corrections using an **LLM-powered GroqClient**.
- It extracts SQL queries from LLM responses, executes them against the database, and logs the results.
- Errors encountered during execution are captured and used to **refine further corrections**.

### 3. **Prompt-Based Query Generation**

- `prompt.py` allows users to manually generate or correct SQL queries by providing natural language descriptions or incorrect SQL statements.

### 4. **Training Data Processing**

- `generate_json.py` processes **training data** into a structured JSON format, useful for fine-tuning or further development.

## 🛠️ Tech Stack

- **Python 3.8+**
- **PostgreSQL** (for schema extraction and SQL execution)
- **GroqClient** (LLM API for SQL correction and generation)
- **Pandas** (for structured data handling)
- **Regex** (for extracting SQL queries from text)
- **Logging** (for structured debugging and monitoring)

## 🏗️ Setup & Installation

### 1️⃣ Clone the Repository

```sh
 git clone https://github.com/MrAyushBajpai/IgnisQL.git
 cd IgnisQL
```

### 2️⃣ Install Dependencies

```sh
 pip install -r requirements.txt
```

### 3️⃣ Set Up Environment Variables

Create a `.env` file with the following details:

```env
DATABASE_URL=postgresql://username:password@localhost:5432/dbname
GROQ_API_KEY=your_groq_api_key_here
```

### 4️⃣ Run the Database Schema Extractor

```sh
python schema_extractor.py
```

This fetches the schema details and prepares them for LLM-based SQL corrections.

### 5️⃣ Run SQL Correction

To correct an incorrect SQL query manually:

```sh
python sql_corrector.py
```

Example test case inside `sql_corrector.py`:

```python
result = corrector.correct_sql("SELECT * FORM customers", execute=True)
print(result)
```

### 6️⃣ Manually Generate or Correct SQL Queries

Run:

```sh
python prompt.py
```

This allows users to enter natural language descriptions or incorrect SQL queries for correction.

### 7️⃣ Generate JSON from Training Data

```sh
python generate_json.py
```

This processes training data into a JSON format for further refinement or LLM training.

## 🎯 Features

✅ **Automatic SQL Error Detection & Correction** ✅ **Database-Aware Query Generation** ✅ **Real-Time SQL Execution & Validation** ✅ **Schema-Driven LLM Prompting** ✅ **Training Data Processing for Model Fine-Tuning**

## 🤝 Contributions

Feel free to submit pull requests, report issues, or suggest enhancements via [GitHub Issues](https://github.com/MrAyushBajpai/IgnisQL/issues).

## 📜 License

This project is licensed under the MIT License.

---

🔗 **GitHub Repo:** [IgnisQL](https://github.com/MrAyushBajpai/IgnisQL)

