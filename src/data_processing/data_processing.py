import pandas as pd
import os
import glob
from collections import Counter

# --- (Keep your existing process_ldap_data function) ---
def process_ldap_data(ldap_path):
    """
    Reads all LDAP files to create a user-to-role map.
    """
    print("Processing LDAP data...")
    ldap_files = glob.glob(os.path.join(ldap_path, '*.csv'))
    if not ldap_files:
        print("Warning: No LDAP files found.")
        return pd.DataFrame(columns=['user_id', 'role'])

    ldap_df = pd.concat((pd.read_csv(f) for f in ldap_files), ignore_index=True)
    ldap_df = ldap_df.rename(columns={'Role': 'role'})
    user_to_role_map = ldap_df[['user_id', 'role']].drop_duplicates(subset='user_id', keep='last')
    print(f"Found roles for {len(user_to_role_map)} unique users.")
    return user_to_role_map


def create_summary_from_events(events):
    """
    A more advanced function to create a concise, summary-style narrative.
    """
    if events.empty:
        return ""

    # --- Extract basic info from the first event ---
    first_event = events.iloc[0]
    user_id = first_event['user_id']
    role = first_event['role']
    # Use mode to find the most frequently used PC for the day's header
    pc = events['pc'].mode().iloc[0] if not events['pc'].empty else 'Unknown PC'
    
    # --- Start the narrative header ---
    narrative_parts = [f"User {user_id} (Role: {role}) on {pc}:"]
    
    # --- Process events chronologically ---
    # To summarize web activity, we'll collect URLs in a buffer
    web_activity_buffer = []

    def flush_web_buffer():
        """Helper to summarize and clear the web activity buffer."""
        if not web_activity_buffer:
            return
        
        start_time = web_activity_buffer[0]['timestamp'].strftime('%H:%M:%S')
        end_time = web_activity_buffer[-1]['timestamp'].strftime('%H:%M:%S')
        total_visits = len(web_activity_buffer)
        
        # Count domain occurrences
        domain_counts = Counter([item['url'].split('//')[-1].split('/')[0] for item in web_activity_buffer])
        top_domains = domain_counts.most_common(3)
        top_domains_str = ", ".join([f"{domain} ({count} times)" for domain, count in top_domains])

        narrative_parts.append(
            f"Between {start_time} and {end_time}, visited {total_visits} websites, including {top_domains_str}."
        )
        web_activity_buffer.clear()

    for _, event in events.iterrows():
        time_str = event['timestamp'].strftime('%H:%M:%S')
        activity_type = event['activity_type']
        
        # If the activity is not web, flush any pending web summary
        if activity_type != 'http':
            flush_web_buffer()

        if activity_type == 'logon':
            narrative_parts.append(f"Logged on at {time_str}.")
        elif activity_type == 'logoff':
            narrative_parts.append(f"Logged off at {time_str}.")
        elif activity_type == 'device_connect':
            narrative_parts.append(f"Connected a USB device at {time_str}.")
        elif activity_type == 'device_disconnect':
            narrative_parts.append(f"Disconnected a USB device at {time_str}.")
        elif activity_type == 'http':
            # Add web event to buffer to be summarized later
            web_activity_buffer.append(event)
            
    # After the loop, flush any remaining web activity
    flush_web_buffer()

    return " ".join(narrative_parts)


def generate_narratives(user_role_map):
    """
    Processes all logs, creates chronological events, and groups them by day
    into a high-quality narrative.
    """
    all_events = []
    
    print("Processing logon.csv...")
    logon_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'logon.csv'))
    logon_df['user_id'] = logon_df['user'].str.split('/').str[1]
    logon_df = pd.merge(logon_df, user_role_map, on='user_id', how='left').fillna({'role': 'Unknown'})
    logon_df['timestamp'] = pd.to_datetime(logon_df['date'])
    logon_df['activity_type'] = logon_df['activity'].apply(lambda x: 'logon' if x == 'Logon' else 'logoff')
    all_events.append(logon_df[['timestamp', 'user_id', 'role', 'pc', 'activity_type']])

    print("Processing device.csv...")
    device_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'device.csv'))
    device_df['user_id'] = device_df['user'].str.split('/').str[1]
    device_df = pd.merge(device_df, user_role_map, on='user_id', how='left').fillna({'role': 'Unknown'})
    device_df['timestamp'] = pd.to_datetime(device_df['date'])
    device_df['activity_type'] = device_df['activity'].apply(lambda x: 'device_connect' if x == 'Connect' else 'device_disconnect')
    all_events.append(device_df[['timestamp', 'user_id', 'role', 'pc', 'activity_type']])
    
    print("Processing http.csv...")
    http_column_names = ['id', 'date', 'user', 'pc', 'url', 'content'] 
    http_df = pd.read_csv(os.path.join(RAW_DATA_PATH, 'http.csv'), header=None, names=http_column_names)
    http_df['user_id'] = http_df['user'].str.split('/').str[1]
    http_df = pd.merge(http_df, user_role_map, on='user_id', how='left').fillna({'role': 'Unknown'})
    http_df['timestamp'] = pd.to_datetime(http_df['date'])
    http_df['activity_type'] = 'http'
    all_events.append(http_df[['timestamp', 'user_id', 'role', 'pc', 'activity_type', 'url']])

    print("Combining and sorting all events...")
    final_df = pd.concat(all_events, ignore_index=True)
    final_df = final_df.sort_values(by=['user_id', 'timestamp'])

    print("Generating summary narratives...")
    final_df['date_only'] = final_df['timestamp'].dt.date
    
    # Apply the advanced summary function to each user-day group
    narratives = final_df.groupby(['user_id', 'date_only']).apply(create_summary_from_events).reset_index(name='narrative')
    
    # Final formatting
    narratives = narratives.rename(columns={'date_only': 'date'})
    final_output = narratives[['date', 'user_id', 'narrative']].rename(columns={'user_id': 'user'})
    
    return final_output

# --- (Keep your existing if __name__ == "__main__": block) ---
if __name__ == "__main__":
    # Assumed project structure from previous conversations
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
    RAW_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'r1')
    PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'r1')
    LDAP_FOLDER_PATH = os.path.join(RAW_DATA_PATH, 'ldap')

    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    
    # 1. Get user roles
    user_roles = process_ldap_data(LDAP_FOLDER_PATH)
    
    # 2. Generate daily narratives
    all_narratives_df = generate_narratives(user_roles)
    
    # 3. Save the final output
    output_filename = os.path.join(PROCESSED_DATA_PATH, 'daily_user_narratives_v2.csv')
    all_narratives_df.to_csv(output_filename, index=False)
    
    print(f"\n✅ Processing complete! Improved narratives saved to: {output_filename}")
    pd.set_option('display.max_colwidth', 400) # To see the full narrative in the sample output
    print("\n--- Sample Output ---")
    print(all_narratives_df.head().to_string())