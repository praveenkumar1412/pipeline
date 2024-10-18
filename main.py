#DATA EXTRACTION CODE FOR GAS
import os
import io
from urllib.parse import urlparse
import fitz  # PyMuPDF
from google.cloud import storage
from gen import extract_and_upload
import argparse
from verification import verification

def upload_to_gcs(bucket_name, file_data, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(file_data, content_type='application/pdf')
    print(f'File {destination_blob_name} uploaded to {bucket_name}.')
    gs_uri = f'gs://{bucket_name}/{destination_blob_name}'
    print(f"uri={gs_uri}")
    
    return gs_uri

def download_pdf_from_gcs(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    pdf_data = blob.download_as_bytes()
    return pdf_data

def count_pages(bucket_name, source_blob_name):
    pdf_data = download_pdf_from_gcs(bucket_name, source_blob_name)
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    total_pages = pdf_document.page_count
    pdf_document.close()
    return total_pages

def split_pdf_with_flexible_overlap(pdf_data, original_filename, pages_per_split=30, overlap=2, bucket_name=None):
    pdf_document = fitz.open(stream=pdf_data, filetype="pdf")
    total_pages = pdf_document.page_count
    base_filename = os.path.splitext(original_filename)[0]

    part_num = 1
    part_urls = []  # To accumulate the URLs of the split parts
    for i in range(0, total_pages, pages_per_split - overlap):
        pdf_writer = fitz.open()
        
        # Calculate the start and end pages for the current chunk
        start_page = i
        end_page = min(i + pages_per_split, total_pages)
        
        for j in range(start_page, end_page):
            pdf_writer.insert_pdf(pdf_document, from_page=j, to_page=j)
        
        output_buffer = io.BytesIO()
        pdf_writer.save(output_buffer)
        output_buffer.seek(0)

        if bucket_name:
            destination_blob_name = f'{base_filename}_part_{part_num}.pdf'
            part_url = upload_to_gcs(bucket_name, output_buffer, destination_blob_name)
            part_urls.append(part_url)

        output_buffer.close()
        part_num += 1

    pdf_document.close()

    return part_urls

def main():
    #Set up argument parsing
    parser = argparse.ArgumentParser(description='Process some arguments.')
    
    # Define the arguments that will be passed to the job
    parser.add_argument('--file', type=str, help='Name of the file')
    parser.add_argument('--bucket', type=str, help='name of the bucket')
    
    # Parse the arguments
    args = parser.parse_args()
    file_name = args.file
    bucket_name = args.bucket
    
    print(f"file: {file_name} bucket: {bucket_name}")

    file_paths = f"gs://{bucket_name}/{file_name}"

    #file_paths = ["gs://training_data26072024/0919203 to 101923 billing_Customer_Bill elc.pdf"]
    #file_paths = "gs://training_data26072024/InvoiceImages - 2024-06-26T151003.094.pdf"  
    token=verification(file_paths)
    print(token)
    print(file_paths)


    if token == "TRUE":

        print(f"gas service :{token}  ")
        uri = file_paths
        parsed_uri = urlparse(uri)
        bucket_name = parsed_uri.netloc
        source_blob_name = parsed_uri.path.lstrip('/')
        original_filename = os.path.basename(source_blob_name)

        print(f"bucket: {bucket_name}  source: {source_blob_name}")

        try:
            no_of_pages = count_pages(bucket_name, source_blob_name)

            if no_of_pages > 30:
                print("PDF has more than 30 pages.")
                pdf_data = download_pdf_from_gcs(bucket_name, source_blob_name)
                part_urls = split_pdf_with_flexible_overlap(pdf_data, original_filename, bucket_name="splited_pdf")
                extract_and_upload(part_urls, source_blob_name)  # Pass all part URLs for extraction and deduplication
            else:
                extract_and_upload([uri], source_blob_name)  # Directly pass the single URL in a list
        except Exception as e:
            print(f"An error occurred during PDF processing: {e}")

    else:
        print("no gas service is present")

if __name__ == "__main__":
    main()
