#GAS SERVICE
import json
import vertexai
from vertexai.generative_models import GenerativeModel, Part
import vertexai.preview.generative_models as generative_models
from google.cloud import storage
from urllib.parse import urlparse
import os
import requests  # Import the requests library
from training import training_bill, training_message

# URL for the POST API
API_URL = "https://postapi-v1-66988718791.us-central1.run.app/add-gas"


def generate_content(url,file_name):
    file_name, ext = os.path.splitext(file_name)
    vertexai.init(project="echelon-data-intake", location="us-east1")
    
    textsi_1 = """you are documnent extractor who the extarct data related to gas service alone.Avoid adding special characters or formatting outside of a valid JSON structure"""
    model = GenerativeModel(
        "gemini-1.5-pro-001",
        system_instruction=[textsi_1]
    )

    document1 = Part.from_uri(mime_type="application/pdf", uri=url)
    training_document = Part.from_uri(mime_type="application/pdf", uri=training_bill)

    message = """
    Extract all the gas service information with the columns named: customer_name, account_number, address, service_start_date, service_end_date,current_reading, previous_reading, meter, total_gas_usage, gas_usage_unit and total_cost in JSON format. Ensure that the extracted information is related to gas service alone. Use the date format YYYY-MM-DD for the date fields. For all other fields, the values should be strings. If any value is missing for a particular column, assign null for that value. Avoid adding special characters or formatting outside of a valid JSON structure.

    """

    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    safety_settings = {
        generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }

    print(f"Processing: {url} now without streaming (individual document processing).")
    
    # Stream is set to False to treat each part as a separate document
    response = model.generate_content(
        [training_document, training_message, document1, message],
        generation_config=generation_config,
        safety_settings=safety_settings,
        stream=False,  # Disable streaming mode to process parts individually
    )

    # Since response is not iterable when stream=False, directly access the response text
    if hasattr(response, 'text'):
        output = response.text
    else:
        output = response  # Fallback, in case response.text does not exist

    combined_text = output.strip("```json").strip("```").strip()

    if not combined_text:
        raise ValueError("The AI model did not return any valid content to process.")

    try:
        # Attempt to parse the generated JSON content
        parsed_data = json.loads(combined_text)

    
        if isinstance(parsed_data, list):
            
            for entry in parsed_data:
              if isinstance(entry, dict):  
                entry["subject"] = file_name #<--- for list
        elif isinstance(parsed_data, dict):
            
            parsed_data["subject"] = file_name
        else:
            raise ValueError(f"Unexpected data format: {type(parsed_data)}")

  
        if isinstance(parsed_data, list):
            return parsed_data
        else:
            return [parsed_data]

    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON. Error: {e}")
        print(f"Raw response content: {combined_text}")
        raise

def remove_duplicates(data):
    unique_entries = []
    seen = set()

    for entry in data:
        # Check if the entry is a dictionary
        if isinstance(entry, dict):
            # Create a tuple of values that define a unique record
            identifier = (
                entry.get("customer_name"),
                entry.get("account_number"),
                entry.get("service_start_date"),
                entry.get("service_end_date"),
                entry.get("meter")
            )

            # If this identifier is not in the seen set, add it to the unique list
            if identifier not in seen:
                unique_entries.append(entry)
                seen.add(identifier)
        else:
            print(f"Unexpected data format: {entry}. Skipping entry.")

    return unique_entries


def upload_to_gcs(bucket_name, destination_blob_name, content):
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(content, content_type='application/json')

    print(f"Content uploaded to {bucket_name}/{destination_blob_name}")


def post_to_api(json_data):
    """Post the JSON data to the external API."""
    try:
        response = requests.post(API_URL, json=json_data)
        if response.status_code == 201:
            print(f"Data successfully posted to {API_URL}")
        else:
            print(f"Failed to post data. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"An error occurred while posting to the API: {e}")


def extract_and_upload(urls, source_blob_name):
    bucket_name = "output_0808json"
    bucket_name1= "splited_pdf"
    all_extracted_data = []
    print(f"Now in extract_and_upload() with URLs: {urls}")

    # Process each URL (split PDF part) and save the output as separate JSON files
    for index, url in enumerate(urls, start=1):
        try:
            print(f"Working on: {url}")
            file_name = os.path.basename(source_blob_name)
            extracted_data = generate_content(url,file_name)
            if extracted_data:
                # Save each part as its own JSON file before combining
                part_file_name = f"{os.path.basename(source_blob_name).replace('.pdf', '')}_part_{index}.json"
                part_json = json.dumps(extracted_data, indent=2)
                
                # Upload each part's JSON file to GCS
                upload_to_gcs(bucket_name1, part_file_name, part_json)
                
               
                all_extracted_data.extend(extracted_data)
            else:
                print(f"No content generated by the AI model for {url}.")
        except Exception as e:
            print(f"An error occurred during content generation: {e}")

    # If we have any extracted data, combine them and save as a single JSON file
    if all_extracted_data:
        unique_data = remove_duplicates(all_extracted_data)
        output_json = json.dumps(unique_data, indent=2)
        
        # Upload the combined JSON to GCS
        combined_file_name = os.path.basename(source_blob_name).replace('.pdf', '_gas.json')
        print(f"Uploading combined JSON: {combined_file_name}")

        upload_to_gcs(bucket_name, combined_file_name, output_json)
        
        # Post the combined JSON data to the API
        post_to_api(unique_data)
    else:
        print("No data to upload after processing all parts.")
