from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

def parse_revision_xml(xml_content: str, include_text: bool = False) -> dict:
    """Parse a single revision XML string into a dictionary."""
    soup = BeautifulSoup(xml_content, "lxml-xml")
    revision = soup.find("revision")
    
    # Extract contributor information safely
    contributor = revision.find("contributor")
    if contributor:
        username = contributor.find("username")
        username = username.text if username else None
        userid = contributor.find("id")
        userid = userid.text if userid else None
    else:
        username = None
        userid = None
    
    # Find text content
    text_elem = revision.find("text")
    text_content = text_elem.text if text_elem else ""
    
    # Extract basic revision information
    data = {
        'revision_id': revision.find("id").text if revision.find("id") else None,
        'timestamp': revision.find("timestamp").text if revision.find("timestamp") else None,
        'username': username,
        'userid': userid,
        'comment': revision.find("comment").text if revision.find("comment") else None,
        'text_length': len(text_content)
    }
    
    # Optionally include the full text content
    if include_text:
        data['text'] = text_content
    
    return data

def process_article_directory(article_dir: Path, batch_size: int = 1000, include_text: bool = False) -> pd.DataFrame:
    """Process all revisions for an article into a single DataFrame."""
    # Collect all XML files recursively in each year/month directory
    xml_files = list(article_dir.glob("*/**/*.xml"))  # Searches within each year/month/subdir structure
    
    print(f"Found {len(xml_files)} XML files in {article_dir}")  # Debugging output
    
    if not xml_files:
        print(f"No XML files found in {article_dir}")
        return None
    
    dataframes = []
    for i in tqdm(range(0, len(xml_files), batch_size),
                  desc=f"Processing {article_dir.name}",
                  unit="batch"):
        batch = xml_files[i:i + batch_size]
        revision_data = []
        
        for file_path in batch:
            try:
                xml_content = file_path.read_text(encoding="utf-8")  # Ensure UTF-8 encoding
                data = parse_revision_xml(xml_content, include_text)
                # Add file path information
                data['year'] = file_path.parent.parent.name  # Assuming parent is the month directory
                data['month'] = file_path.parent.name         # Assuming parent of the XML file is the month
                revision_data.append(data)
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
        
        if revision_data:
            dataframes.append(pd.DataFrame(revision_data))
    
    if not dataframes:
        return None
    
    final_df = pd.concat(dataframes, ignore_index=True)
    final_df['timestamp'] = pd.to_datetime(final_df['timestamp'])
    return final_df.sort_values('timestamp', ascending=False)

def main(data_dir: str, output_dir: str, batch_size: int = 1000, include_text: bool = False):
    """
    Process all article directories into a single DataFrame and save as one feather file.
    """
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Processing with {'text content' if include_text else 'text length only'}")
    
    # Aggregate all data from each article directory into a single DataFrame
    combined_dataframes = []
    
    for article_dir in data_dir.iterdir():
        if not article_dir.is_dir():
            continue
        
        print(f"Starting processing for {article_dir.name}")
        df = process_article_directory(article_dir, batch_size, include_text)
        
        if df is not None:
            combined_dataframes.append(df)
        else:
            print(f"No data found for {article_dir.name}")
    
    # Combine all article data into one DataFrame
    if combined_dataframes:
        final_combined_df = pd.concat(combined_dataframes, ignore_index=True)
        output_path = output_dir / f"{data_dir.name}.feather"
        final_combined_df.to_feather(output_path)
        print(f"Combined DataFrame saved at {output_path}")
    else:
        print("No data found in any article directory.")

# Example of how to call the main function directly
if __name__ == "__main__":
    # Replace these paths and parameters with your actual values
    data_directory = "C:/Users/shil6369/Documents/GitHub/oii-fsds-wikipedia/data/Oblast"
    output_directory = "C:/Users/shil6369/Documents/GitHub/oii-fsds-wikipedia/DataFrames"
    batch_size = 1000
    include_text = True
    
    main(data_directory, output_directory, batch_size, include_text)
