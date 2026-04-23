import pandas as pd

BUSINESS_CODES = {
    'lawn':        ['C27', 'D49'],
    'general':     ['A', 'B'],
    'plumber':     ['C36'],
    'handyman':    ['B'],
    'hvac':        ['C20', 'C38'],
    'electrician': ['C10'],
}


def normalize(code):
    #Hyphens and spaces are cosmetic thus rip them out so 'C-6' and 'C6' become the same symbol.
    return code.replace('-', '').replace(' ', '').upper()


def row_matches(cell, wanted_codes):
    #A blank cell has nothing to match against, so the row can't be a keeper
    if pd.isna(cell):
        return False
    
    #The state jams multiple codes into one cell separated by '|', so splitting gives the pieces.
    pieces = cell.split('|')
    
    #Compare cleaned-up codes to cleaned-up codes — otherwise 'C-10 ' would miss 'C10' 
    for piece in pieces:
        if normalize(piece) in wanted_codes:
            return True
    return False


def filter_by_trade(df, business_type):
    #Normalize the target codes once up front so we aren't redoing the same work on every row.
    wanted = [normalize(c) for c in BUSINESS_CODES[business_type]]
    
    #Build a True/False flag per row — True means this row's trade code is one we want.
    keep = []
    for cell in df['Classifications(s)']:
        keep.append(row_matches(cell, wanted))
    
    #Feeding pandas the bool list slices out exactly the rows we flagged True.
    return df[keep]


def keep_only_active(df):
    return df[df['PrimaryStatus'] == 'CLEAR']


def keep_only_with_phone(df):
    #notna() kills empty cells and the .str.strip() != '' catches cells that are only spaces — both mean 'no way to contact them'.
    return df[df['BusinessPhone'].notna() & (df['BusinessPhone'].str.strip() != '')]


def clean(input_csv):
    #Reading as string keeps phone numbers, ZIPs, and leading zeros intact instead of turning them into floats.
    df = pd.read_csv(input_csv, dtype=str)
    
    #This bucket collects one mini-DataFrame per trade, glue them all together at the end.
    pieces = []
    for trade in BUSINESS_CODES:
        #Three sieves in series — wrong trade, dead license, or no phone drops the row out of the pipeline.
        matched = filter_by_trade(df, trade)
        matched = keep_only_active(matched)
        matched = keep_only_with_phone(matched)

        #Keep only the four columns that matter for outreach — everything else is noise for this purpose.
        trimmed = matched[['BusinessName', 'BusinessPhone', 'City']].copy()
        
        #Tag every surviving row with which trade bucket it came from, so the combined file still tells us the type.
        trimmed['BusinessType'] = trade
        pieces.append(trimmed)
        print(f'{trade:12s} -> {len(trimmed):4d} rows')
    #concat stacks all the per-trade tables vertically into one master list and ignore_index renumbers the rows so they're 0..N.
    return pd.concat(pieces, ignore_index=True)

def main() -> None:
    csv_name = input('Enter the name of your csv: ')

    #One call does all six trades and hands back a single combined DataFrame.
    result = clean(f'step-1/raw-csvs/{csv_name}.csv')

    #write the master file once at the end
    result.to_csv(f'step-2/filtered-csvs/{csv_name}-cleanedcsv', index=False)
    print(f'total        -> {len(result):4d} rows written')

if __name__ == '__main__':
    main() 