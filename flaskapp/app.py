from flask import Flask, request, jsonify
import snowflake.connector
import os
from flask_cors import CORS
import requests
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

# Function to get a Snowflake connection
def get_db_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

# Route to handle user query, generate SQL, and fetch results
@app.route('/process-query', methods=['POST'])
def process_query(): 
    previous = "" 
    try:
        # Parse user query
        message_history = request.json.get('messages', [])
        print(f" $$$$$$$$$$$$$$$$ message_history: {message_history}")
    
        if not message_history:
            return jsonify({"error": "No message history provided"}), 1000

        # Step 1: Send user query to GPT to generate SQL query
        system_message_1 = ("You are an SQL query generator for user queries from a chatbox on a website dedicated to Andrew Huberman and his podcast videos. "
    "Your task is to analyze user input, extract relevant keywords, and always generate an SQL query to fetch data from the Snowflake table `FN_BIGTABLE`.\n\n"

    "### Table Schema Overview:\n"
    "`FN_BIGTABLE` contains the following columns:\n"
    "- VIDEOID: Unique identifier for the video.\n"
    "- TITLE: Title of the video (lowercase).\n"
    "- DESCRIPTION: Short description of the video (lowercase).\n"
    "- THUMBNAILIMAGEURL: URL to the thumbnail image.\n"
    "- VIDEOLINK: Direct URL to the video.\n"
    "- VIDEOCATEGORY: Video type (lowercase) (e.g., 'with guest', 'solo', 'live event', 'ama', 'essentials').\n"
    "- GUEST: Guest(s) featured in the video (lowercase).\n"
    "- PUBLISHEDDATE: Date the video was published.\n"
    "- DURATION: Length of the video.\n"
    "- VIEWCOUNT: Number of views the video has.\n"
    "- LIKESCOUNT: Number of likes the video has.\n"
    "- COMMENTSCOUNT: Number of comments on the video.\n"
    "- POSITIVECOMMENTSPERCENTAGE: Percentage of positive comments.\n"
    "- NEGATIVECOMMENTSPERCENTAGE: Percentage of negative comments.\n"
    "- NEUTRALCOMMENTSPERCENTAGE: Percentage of neutral comments.\n"
    "- TOPIC: Main topic or theme of the video (lowercase) (e.g., 'focus', 'neuroplasticity', 'mental health & addiction', 'focus, productivity & creativity', 'neuroplasticity, habits & goals', 'neurobiology & physiology', 'hormone health', 'general insights from guest', 'learning & memory', 'andrew huberman content', 'emotions & relationships', 'heat & cold exposure', 'gut health', 'light exposure & circadian rhythm', 'aging & longevity', 'fertility', 'fitness & recovery', 'adhd, drive & motivation', 'sleep', 'caffeine science', 'general knowledge & health', 'the science of well-being', 'nutrition', 'mental health & addiction', 'supplements', 'alcohol, tobacco & cannabis', 'metabolism & immunity', 'nsdr, meditation & rest').\n\n"

    "### Rules for Query Generation:\n"
    "1. **Always Generate a Query:**\n"
    "   - For every user query, attempt to construct an SQL query that fetches relevant data from `FN_BIGTABLE`.\n"
    "   - Even if the query is vague or ambiguous, use general terms like 'health', 'productivity', or 'focus' to generate a meaningful query.\n\n"
    "2. **Keyword Extraction:**\n"
    "   - Extract relevant keywords from the user input (e.g., 'AMA', 'focus', 'Jeff Cavaliere').\n"
    "   - Map the extracted keywords to relevant columns such as `TOPIC`, `TITLE`, `DESCRIPTION`, or `VIDEOCATEGORY`.\n\n"
    "3. **Search Scope:**\n"
    "   - Use the `LIKE` operator with wildcards (`%`) for partial matches (e.g., `LIKE '%ama%'`).\n"
    "   - Combine multiple keywords using `OR` to broaden the search.\n"
    "   - Use only single word in search Keyword (Eg '%brain%'). Do not use multiple keywords with spaces(Eg'nutrients & Brain health%').\n"
    "   - use more than 2 columns WITH OR Clause in where condition while searching. you can get best results if you search TITLE, DESCRIPTION, TOPIC, GUEST WITH OR conditions\n"
    "   - Do not use AND conditions \n\n"
    "4. **Specific Queries for Common Questions:**\n"
    "   - If the user asks about 'topics' or general categories, fetch distinct values from the `TOPIC` column:\n"
    "     SELECT DISTINCT TOPIC FROM FN_BIGTABLE ORDER BY TOPIC;\n\n"
    "5. **Ordering Results:**\n"
    "   - Prioritize results by relevance using `ORDER BY` clauses (e.g., LIKESCOUNT, VIEWCOUNT, or POSITIVECOMMENTSPERCENTAGE).\n\n"
    "6. **Pagination:**\n"
    "   - Include a `LIMIT` clause to restrict results to 10 rows.\n\n"
    "7. **Fallback Handling:**\n"
    "   - If no specific keywords can be extracted, assume broad terms and generate a query using them.\n"
    "   - If the query is off-topic (unrelated to Andrew Huberman's podcast), return: 'Your question should focus on Andrew Huberman or his videos.'\n\n"

    "### Example Queries:\n"
    "1. For 'What topics does he cover?':\n"
    "   SELECT DISTINCT TOPIC FROM FN_BIGTABLE ORDER BY TOPIC;\n\n"

    "2. For 'What is AMA in the dashboard?':\n"
    "   SELECT * FROM FN_BIGTABLE WHERE LOWER(TITLE) LIKE '%ama%' OR LOWER(DESCRIPTION) LIKE '%ama%' OR LOWER(TOPIC) LIKE '%ama%' ORDER BY VIEWCOUNT DESC LIMIT 10;\n\n"

    "3. For 'videos on focus and productivity':\n"
    "   SELECT * FROM FN_BIGTABLE WHERE LOWER(TOPIC) LIKE '%focus%' OR LOWER(TOPIC) LIKE '%productivity%' ORDER BY VIEWCOUNT DESC LIMIT 10;\n\n"

    "4. For 'most liked videos with guests':\n"
    "   SELECT * FROM FN_BIGTABLE WHERE LOWER(VIDEOCATEGORY) = 'with guest' ORDER BY LIKESCOUNT DESC LIMIT 10;\n\n"

    "### Important Notes:\n"
    "- Always generate a query, regardless of the user query's clarity.\n"
    "- Use broad keywords if no specific terms are provided.\n"
    "- Ensure the SQL query is valid and aligned with the `FN_BIGTABLE` schema.\n"
    "- Return only the SQL query, without explanations or additional text.\n"
    "- Remember the query you send will go directly to snowflake connection to fetch data so, no spaces at the start and end of response.\n\n"
    "  SEND ONLY SQL QUERY\n")
        
        apikey = os.getenv('OPEN_AI_API')
        client = OpenAI(api_key=apikey)
        
        gpt_sql_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message_1}] + message_history,
            temperature=0.0)
        response_content = gpt_sql_response.choices[0].message.content.strip()
        
        print(f"###############   first response : {response_content}")

        if not response_content:
            return jsonify({"error": "GPT did not generate a valid SQL query."}), 500

        # Step 2: Execute the generated SQL query on Snowflake
        data = []
        if response_content.startswith("SELECT"):
            conn = get_db_connection()
            if not conn:
                return jsonify({"error": "Unable to connect to Snowflake."}), 500

            cursor = conn.cursor()
            cursor.execute(response_content)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
            if data == []:
                print("No data found in the table")
                cursor.execute("SELECT * FROM FN_RK1_TABLE")
                rows = cursor.fetchall()
                data = [dict(zip(columns, row)) for row in rows]
            cursor.close()
            conn.close()
            print(f"###############################len of data : {(len(data))}")
            
        # Step 3: Send the resultant data to GPT for answering the user's query
        system_message_2 = (
    "You are an AI assistant for a chatbox on a huberma lab analytics dashboard website  dedicated to Andrew Huberman and his podcast videos. "
    "Your task is to analyze user queries and help them discover the perfect video from the fetched data, while providing accurate responses based on their input or insights from the embedded Power BI dashboard.\n\n"

    "### Context and Intent Analysis:\n"
    "1. **General Descriptions**:\n"
    "   - if its a intro question like Hi, Hello, greet nicely and tell them about the website and what you can do for them.\n"
    "   - If the user asks general questions (e.g., 'Who is Andrew Huberman?'), provide a concise summary about him or his work. Ensure the response is less than 120 words.\n"
    "2. **Topic-Specific Inquiries**:\n"
    "   - For questions like 'Does he discuss mental health?', confirm using the fetched data and, if relevant, ask if the user would like to see a video recommendation.\n"
    "3. **Finding the Perfect Video**:\n"
    "   - If the user’s question implies they are looking for a video (e.g., 'What should I watch about stress management?' or 'Recommend something on focus and productivity'), analyze their input and:\n"
    "     - Use the fetched data to identify videos matching their interest.\n"
    "     - Ask the user if they would like to receive the video details (e.g., 'Would you like me to share the video link and thumbnail?').\n"
    "     - Only share the video link and thumbnail if the user explicitly requests it.\n"
    "4. **Video-Specific Inquiries**:\n"
    "   - If a user asks about a specific video (e.g., 'What's the video on psilocybin benefits about?'), use the fetched data to provide a detailed description.\n"
    "   - End the response with: 'If you'd like, I can share the video link and thumbnail.'\n"
    "   - Only provide the link and thumbnail if the user explicitly confirms.\n"
    "5. **Dashboard-Related Questions**:\n"
    "   - For questions about the Power BI dashboard (e.g., 'What does the chart on video views mean?' or 'Which video has the highest positive sentiment?'):\n"
    "     - Use precise language to interpret the dashboard's insights (e.g., explaining trends, patterns, or outliers).\n"
    "     - Ensure the response is less than 120 words.\n"
    "     - If the query is vague, clarify by asking what specific aspect of the dashboard they want to understand.\n\n"

    "### Response Format:\n"
    "1. For text-only responses (less than 120 words):\n"
    "   - Example: 'Andrew Huberman is a neuroscientist who discusses mental health, stress, and productivity in his podcasts.'\n\n"
    "2. For video-specific questions, provide clear and detailed information using the data:\n"
    "   - Example: 'The video on psilocybin benefits discusses its impact on mental health, including treating depression and PTSD. If you'd like, I can share the video link and thumbnail.'\n\n"
    "3. For video recommendations, you must strictly follow this format:\n"
    "   '<response_text>', '<thumbnail_url>', '<video_url>'\n"
    "   - Example:\n"
    "     'Here’s the video on nutrition featuring Dr. Layne Norton: The Science of Eating for Health, Fat Loss & Lean Muscle.', 'https://i.ytimg.com/vi/K4Ze-Sp6aUE/maxresdefault.jpg', 'https://www.youtube.com/watch?v=K4Ze-Sp6aUE''\n"
    "   - **Important:** Every part of the response must be enclosed in single quotes (`'`). Ensure there are no missing or extra quotes. Deviation from this structure is not acceptable.\n\n"
    "4. **Dashboard-Related Responses (less than 120 words):**\n"
    "   - Example: 'The dashboard shows viewer sentiment analysis, indicating audience satisfaction and popular topics.'\n\n"


    "### Guidelines:\n"
    "- Always aim to help the user find the perfect video based on their interests or questions.\n"
    "- Use the fetched data to recommend videos that are most relevant and popular.\n"
    "- Provide concise, definitive answers using the available data. Avoid speculative terms like 'likely' or 'might.'\n"
    "- Proactively ask the user if they would like the video link and thumbnail. Only share them if the user explicitly confirms their interest.\n"
    "- Text-only responses should always be less than 120 words.\n"
    "- Make sure you give the correct thumnail url and video url relevant to the video you are recommending.\n"
    "- Use the exact format for video recommendations: '<response_text>', '<thumbnail_url>', '<video_url>'. Do not deviate from this format.\n"
    "- Every part of the video recommendation must be enclosed in single quotes. Do not leave any part unquoted.\n"
    "- Stay focused on Andrew Huberman, his podcast videos, or the Power BI dashboard.\n"
    "- Use clarity, precision, and relevance in your responses.\n"
    "- Avoid empty or broken links in video recommendations.\n"
    "- Do not answer anything which is not related to andrew huberman. Always stay in the context.\n\n"

    "Here is the data fetched by the other AI model for this context. Use it for crafting responses.:\n"
    ""f'{data}'"\n\n"

    "Based on the user query and fetched data, generate the most appropriate response. Ensure text responses are less than 120 words and video recommendations are always provided in the exact specified format."
)
        
        gpt_answer_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message_2}] + message_history,
            temperature=0.1)
        

        final_response_content = gpt_answer_response.choices[0].message.content.strip()
        print(f"###############   final_response_content: {final_response_content}")
        return jsonify({"response": final_response_content})

    except Exception as e:
        print(f"Error during processing: {e}")
        return jsonify({"error": "An error occurred while processing the request."}), 500

if __name__ == '__main__':
    # Flask running on port 5000 by default
    app.run(debug=True)