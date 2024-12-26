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
        #request_data = request.get_json()
        #user_query = request_data.get('query')
        message_history = request.json.get('messages', [])
        print(f" $$$$$$$$$$$$$$$$ message_history: {message_history}")
       # if not user_query:
        #    return jsonify({"error": "Query is required."}), 400
        
        if not message_history:
            return jsonify({"error": "No message history provided"}), 400

        # Step 1: Send user query to GPT to generate SQL query
        system_message_1 = (
        "You are an SQL query generator for user input from a chatbox on a website focused on Andrew Huberman and his podcast videos. "
        "Your task is to generate the most relevant SQL query based on user input to fetch data from a Snowflake table (`FN_BIGTABLE`).\n\n"

        "Your role is the first step in an AI workflow. Your responses will be directly used to query a Snowflake table. "
        "You will process user queries to generate SQL queries for fetching data from the table `FN_BIGTABLE`. "
        "This table contains the following columns:\n"
        "- videoid: Unique identifier for the video.\n"
        "- title: Title of the video (lowercase).\n"
        "- description: Short description of the video (lowercase).\n"
        "- thumbnailimageurl: URL to the thumbnail image.\n"
        "- videolink: Direct URL to the video.\n"
        "- videocategory: Category of the video (lowercase) (e.g., 'with guest', 'live event', 'solo', 'ama', 'essentials'). Here solo means its a solo episode by huberman itself, ama means ask me anything episode, live event means its a live event posted\n"
        "- guest: Guest(s) featured in the video (lowercase).\n"
        "- publisheddate: Date the video was published.\n"
        "- duration: Length of the video.\n"
        "- viewcount: Number of views the video has.\n"
        "- likescount: Number of likes the video has.\n"
        "- commentscount: Number of comments on the video.\n"
        "- positivecommentspercentage: Percentage of positive comments.\n"
        "- negativecommentspercentage: Percentage of negative comments.\n"
        "- neutralcommentspercentage: Percentage of neutral comments.\n"
        "- topic: Main topic or theme of the video (lowercase) (e.g., 'focus', 'neuroplasticity', 'mental health & addiction', 'focus, productivity & creativity', 'neuroplasticity, habits & goals', 'neurobiology & physiology', 'hormone health', 'general insights from guest', 'learning & memory', 'andrew huberman content', 'emotions & relationships', 'heat & cold exposure', 'gut health', 'light exposure & circadian rhythm', 'aging & longevity', 'fertility', 'fitness & recovery', 'adhd, drive & motivation', 'sleep', 'caffeine science', 'general knowledge & health', 'the science of well-being', 'nutrition', 'mental health & addiction', 'supplements', 'alcohol, tobacco & cannabis', 'metabolism & immunity', 'nsdr, meditation & rest').\n\n"

        "Analyze the user's query and generate a valid SQL query limited to 10 records.\n\n"

        "- **Rules for SQL Query Generation**:\n"
        "  1. Always ensure the query includes lowercase search terms for `title`, `description`, or `topic`. Example:\n"
        "     SELECT * FROM FN_BIGTABLE WHERE LOWER(TITLE) LIKE '%fitness%' OR LOWER(DESCRIPTION) LIKE '%motivation%' LIMIT 10;\n"
        "  2. Use `ORDER BY` clauses (e.g., LIKESCOUNT, VIEWCOUNT, POSITIVECOMMENTSPERCENTAGE) to prioritize the most relevant results.\n"
        "  3. Combine multiple keywords using `OR` where applicable.\n"
        "  4. Always include a `LIMIT` clause to restrict results to 10 rows.\n"
        "  5. Ensure proper SQL syntax and capitalization. Do not include typos (e.g., avoid 'LIMMIT').\n\n"

        "- **Fallback Handling**:\n"
        "  - If the user query is off-topic or irrelevant to Andrew Huberman's videos, return a reponse to ask them only about huberman or his videos'>\n"
        "  - If the query is ambiguous or unclear, clarify by generating a query with the most general search terms.\n\n"

        "Final Note:\n"
        "  - Your task is strictly to generate SQL queries. Do not provide any additional text or explanations.\n"
        "  - Stay in context and only generate queries about Andrew Huberman and his podcast videos."
)
    


           


        apikey = os.getenv('OPEN_AI_API')
        client = OpenAI(api_key=apikey)
        gpt_sql_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message_1}] + message_history,
            temperature=0.1)
    

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
            cursor.close()
            conn.close()
            print(f"###############################len of data : {(len(data))}")
        # Step 3: Send the resultant data to GPT for answering the user's query
        system_message_2 = (
        "You are an AI assistant for a chat box helping users explore Andrew Huberman's Huberman Lab podcast videos. "
        "You are a chatbox on a front-end website beside a PowerBI dashboard that helps answer questions specifically about Andrew Huberman and his podcast videos. "
        "You must only answer questions related to Andrew Huberman or his podcast videos. If a user asks about any other topic, politely redirect them to ask about Andrew Huberman and his podcast videos.\n\n"

        "For random questions like 'Who is he?' or 'Where is he from?', treat them as inquiries about Andrew Huberman and provide a concise descriptive response.\n"
        "If the user greets with 'Hi', 'Hello', or 'How are you?', respond nicely with an introduction and explain how you can assist them.\n\n"

        "First, determine the intent of the user's query:\n"
        "- If the user is asking for general descriptive information (e.g., 'What is Andrew Huberman known for?'), provide a concise and descriptive text response about Andrew Huberman and his podcast videos.\n"
        "- If the user is explicitly asking for a video recommendation or the query implies they need help finding a video (e.g., 'What should I watch about stress management?'), recommend a video based on the provided data.\n"
        "- For specific questions about whether Andrew Huberman has covered a particular topic (e.g., 'Did he make videos on cannabis?'), answer descriptively (e.g., 'Yes, Andrew Huberman has covered cannabis in his videos.'), and recommend a video only if explicitly requested by the user (e.g., 'Can you recommend one?').\n\n"

        "When recommending a video, use the following strict format:\n"
        "  <'your reply in text','thumbnail_url','video_url'>\n"
        "  Example: 'Check out David Goggins: How to Build Immense Inner Strength.','https://i.ytimg.com/vi/nDLb8_wgX50/maxresdefault.jpg','https://www.youtube.com/watch?v=nDLb8_wgX50'\n\n"

        "If you cannot recommend a video, provide a descriptive response that remains focused on Andrew Huberman's podcast videos. If the user's query is unrelated, respond with: 'Please ask questions about Andrew Huberman or his podcast videos.'\n\n"

        "Here is the data fetched by the other AI model for all user queries in this context. Use it to recommend a video only when required:\n"
            f"'{data}'\n\n"

        "- Do not recommend a video unless:\n"
        "  1. The user explicitly asks for a recommendation (e.g., 'Can you suggest a video?').\n"
        "  2. The user's query strongly implies they need a video recommendation.\n"
        "  3. Relevant data with VIDEOID and THUMBNAILIMAGEURL is available to support the recommendation.\n\n"

        "Focus on clarity, relevance, and precision in responses. Ensure all responses are tailored to the user's query.\n"
        "Use the format mentioned above very strictly ('your reply in text','thumbnail_url','video_url') if responding with a thumbnail URL and video link when you want to recommend a video.\n\n"
        "Ensure descriptive responses do not include empty thumbnail or video links.\n\n"

        "Always stay on-topic and ensure your responses enhance the user’s understanding or experience with Andrew Huberman's content."
        )

        #print(system_message_2)
        
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
    # Flask will run on port 5000 by default
    app.run(debug=True)