from api_keys import get_places_api_key 
import time, requests, os
import pandas as pd 

API_KEY_PLACES: str = get_places_api_key() 
url_places: str = 'https://places.googleapis.com/v1/places:searchText' 
#Header for the POST request for places API
headers: dict[str, str] = {
    'Content-Type': 'application/json',
    'X-Goog-Api-Key': API_KEY_PLACES,
    'X-Goog-FieldMask': 'places.displayName,places.websiteUri,places.nationalPhoneNumber,places.userRatingCount,places.photos,nextPageToken'
}

def normalize_phone(phone: str) -> str:
    #Strip everything non-numeric so '(555) 123-4567' from the csv and '5551234567' from GBP collapse to the same string
    if not phone:
        return ''
    cleaned: str = ''
    for char in str(phone):
        if char.isdigit():
            cleaned += char
    return cleaned

def find_gbp(name: str, city: str, phone: str) -> dict:
    data: dict = {
        'textQuery': f'{name}',
        'pageSize': 20,
    }
    response: requests.models.Response = requests.post(url_places, json=data, headers=headers)

    if response.status_code != 200:
        print(f'API error {response.status_code}: {response.text}')
        return {}

    results: dict = response.json()
    places: list = results.get('places', [])
    if not places:
        return {}

    #Walk candidates and keep the one whose phone matches
    target: str = normalize_phone(phone)
    for place in places:
        candidate: str = normalize_phone(place.get('nationalPhoneNumber', ''))
        if candidate == target:
            return place

    #Got results but none matched the phone 
    return {}


def qualifies(place: dict) -> bool:
    website: str = place.get('websiteUri', '')
    if website:
        return False

    reviews: int = place.get('userRatingCount', 0)
    if reviews < 3:
        return False

    photos: list = place.get('photos', [])
    if len(photos) < 2:
        return False

    return True


def save_leads(keepers: list, output_path: str) -> None:
    #Pulled into its own function so the per-lead flush and the final save use the exact same code path
    result: pd.DataFrame = pd.DataFrame(keepers)
    result.to_csv(output_path, index=False)


def save_remaining(df: pd.DataFrame, processed_indices: list, input_path: str) -> None:
    #Drop the rows we searched, write what's left back to the SAME input file so the next run picks up where this one stopped
    remaining: pd.DataFrame = df.drop(processed_indices)
    remaining.to_csv(input_path, index=False)


def load_prior_leads(output_path: str) -> list:
    #Load existing leads if the file is there, so repeat runs append instead of wiping previous results
    if not os.path.exists(output_path):
        return []
    prior: pd.DataFrame = pd.read_csv(output_path, dtype=str)
    keepers: list = prior.to_dict('records')
    print(f'Loaded {len(keepers)} existing leads from previous runs')
    return keepers


def process_row(row: pd.Series, keepers: list) -> bool:
    #Returns True if the row produced a new lead, False otherwise — main() uses that to decide whether to flush the csv
    name: str = row['BusinessName']
    phone: str = row['BusinessPhone']
    city: str = row['City']
    biz_type: str = row.get('BusinessType', '')

    place: dict = find_gbp(name, city, phone)

    #Empty dict = no GBP hit OR phone didn't match any candidate — either way, skip this row
    if not place:
        print('  skip — no GBP / phone mismatch')
        return False

    reviews: int = place.get('userRatingCount', 0)
    photos: list = place.get('photos', [])
    website: str = place.get('websiteUri', '')

    if not qualifies(place):
        print(f'  skip — reviews={reviews}, photos={len(photos)}, has_site={bool(website)}')
        return False

    keepers.append({
        'BusinessName': name,
        'BusinessPhone': phone,
        'City': city,
        'BusinessType': biz_type,
        'ReviewCount': reviews,
        'PhotoCount': len(photos),
    })
    print(f'  qualifies — reviews={reviews}, photos={len(photos)}')
    return True

def main() -> None:
    csv_name: str = input('Enter the name of your cleaned csv to filter: ')
    search_amount: int = int(input('Enter the number of businesses to filter this run: '))
    
    input_path: str = f'step-2/cleaned-csvs/{csv_name}.csv' 
    output_path: str = f'step-2/leads/{csv_name}-and-filtered.csv' 

    #dtype=str preserves leading zeros in phone numbers that pandas would otherwise nuke into floats
    df: pd.DataFrame = pd.read_csv(input_path, dtype=str)
    keepers: list = load_prior_leads(output_path)
    
    #Snapshot starting size so we can report new-leads-this-run vs total-on-disk separately at the end
    starting_count: int = len(keepers)

    #Clamp the request if the user asked for more rows than the csv actually has
    total_available: int = len(df) 
    run_count: int = min(search_amount, total_available)
    print(f'Running on {run_count} of {total_available} available businesses\n')

    api_requests: int = 0
    #Track which df rows we actually touched, so at the end we can drop exactly those from the input csv
    processed_indices: list = []

    #Iterate only the first run_count rows — iloc gives positional slicing regardless of the index values
    for pos in range(run_count):
        row: pd.Series = df.iloc[pos]
        row_index = df.index[pos]
        print(f'[{pos+1}/{run_count}] {row["BusinessName"]} — {row["City"]}')

        #Mark this row as processed BEFORE any skip path inside process_row, so even skipped rows get removed from the input csv
        processed_indices.append(row_index)
        api_requests += 1

        got_lead: bool = process_row(row, keepers)

        #Flush to disk on every new lead so a mid-run crash never loses an api hit we already paid for
        if got_lead:
            save_leads(keepers, output_path)
            print(f'  saved ({len(keepers)} total leads on disk)')

        #Light throttle so thousands of rows don't slam the endpoint back to back
        time.sleep(0.1)

    #Final save ensures the leads file reflects the final state, and rewrites input csv without processed rows
    save_leads(keepers, output_path)
    save_remaining(df, processed_indices, input_path)
    new_leads: int = len(keepers) - starting_count
    print(f'\nTotal API requests: {api_requests}')
    print(f'New leads this run: {new_leads}')
    print(f'Total leads on disk: {len(keepers)} (in {output_path})')
    print(f'Removed {len(processed_indices)} searched rows from {input_path}')

if __name__ == '__main__':
    main()