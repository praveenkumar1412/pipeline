import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting, FinishReason
import vertexai.generative_models as generative_models

def generate(uri):
    # Initialize the Vertex AI environment
    vertexai.init(project="echelon-data-intake", location="us-central1")

    text1 = """Check the document for gas service. If gas service is present, return True; if gas service is absent, return False. Ensure the response is in Boolean format only."""

    textsi_1 = """You are a document analyzer who checks for gas service in the provided document. If gas service is present, return True. If gas service is not present, return False. The reply must be in Boolean format without any additional response."""

    # Configuration for the model's response generation
    generation_config = {
        "max_output_tokens": 8192,
        "temperature": 1,
        "top_p": 0.95,
    }

    # Define safety settings to ensure appropriate content is returned
    safety_settings = {
        generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
        generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
    }


    # Define the model
    model = GenerativeModel(
        "gemini-1.5-flash-001",system_instruction=[textsi_1]
    )

    # Load the document from the given URI
    document1 = Part.from_uri(
        mime_type="application/pdf",
        uri=uri
    )

    # Generate content using the provided document and instruction (streaming off)
    response = model.generate_content(
        [document1, text1],
        generation_config=generation_config,
        safety_settings=safety_settings,
        stream=False  # Streaming is off
    )

    # Return the generated response text directly
    return response.text

def verification(uri):
    # Call the generate function to analyze the document
    result = generate(uri)
    
   # removing space and converting to all caps  to avoid case sensitivity  problem
    token = result.strip().upper()
    
   
    return token

# Example usage
# uri = "gs://training_data26072024/0919203 to 101923 billing_Customer_Bill elc.pdf"
# verification_token = verification(uri)
# print(verification_token)
