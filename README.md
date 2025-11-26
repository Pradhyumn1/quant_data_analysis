# NIFTY-50 Data Processing Project

This project converts big CSV files into small feather files for NIFTY-50 stocks.

## What This Project Does

Takes 1 big file → Makes 50 small files (one for each stock)

**Input:** `GFDLNFO_BACKADJUSTED_31102025.csv` 
**Output:** 50 feather files in `nifty50_processed/` folder

---

## Files in This Project

### Main Scripts

1. **`process_to_feather.py`** - Main script
   - Makes files that match the sample file exactly
   - Uses same columns as `ACC_2024-05-07.feather`
   - Output: `nifty50_processed/` folder

2. **`full_analysis.py`** - Alternative script
   - Makes files with all available data
   - Creates columns based on actual prices
   - Output: `nifty50_analysis/` folder



---

## How to Run

### Step 1: Install Requirements

```bash
pip install pandas pyarrow
```

### Step 2: Run Main Script

```bash
python3 process_to_feather.py
```

**Wait time:** About 2-3 minutes

**Output:** 50 feather files in `nifty50_processed/` folder

### Step 3: (Optional) Convert to CSV

for check the how the data look like 



## My Approach (How I Solved This)

### Problem
- Big CSV file has data for many stocks mixed together
- Need separate files for each NIFTY-50 stock
- Files must match the sample structure exactly

### Solution Steps

#### Step 1: Load Data
```
Read big CSV → Load into memory → Filter by stock symbol
```

#### Step 2: Parse Ticker Names
Each ticker looks like: `TCS25NOV252600CE.NFO`

I split this into parts:
- `TCS` = Stock symbol
- `25NOV25` = Expiry date
- `2600` = Strike price
- `CE` = Call option (PE = Put option)

#### Step 3: Create Wide Format
The CSV is "long" (many rows):
```
Ticker              | Time     | Close
TCS25NOV252600CE   | 09:15:59 | 467.0
TCS25NOV252600PE   | 09:15:59 | 25.0
```

I convert to "wide" (many columns):
```
Time     | 2600CE_Close | 2600PE_Close
09:15:59 | 467.0        | 25.0
```

#### Step 4: Add Futures Data
Futures are named: `TCS-I.NFO`, `TCS-II.NFO`, `TCS-III.NFO`

I map them to: `FUT_I`, `FUT_II`, `FUT_III`

#### Step 5: Match Sample Structure
- Copy all columns from sample file
- Keep same order
- Fill with data where available
- Leave empty (NaN) where not available

#### Step 6: Save Files
Save each stock as: `SYMBOL_2025-10-31.feather`

---

## Key Challenges & Solutions

### Challenge 1: Strike Price Mismatch
**Problem:** Sample file has strikes 2100-3160. But some stocks trade at 500.  
**Solution:** In strict mode, we keep sample columns (some will be empty). In dynamic mode, we create new columns.

### Challenge 2: Float vs Integer Bug
**Problem:** Strike prices were stored as `2600.0` instead of `2600`. This created wrong column names like `2600.0CE_Close`.  
**Solution:** Convert to integer: `int(strike)` before creating column names.

### Challenge 3: Future Contract Naming
**Problem:** Futures use `-I`, `-II`, `-III` format, not date format.  
**Solution:** Parse the hyphen pattern separately and map to `FUT_I`, `FUT_II`, `FUT_III`.

---

## Output Files

### Strict Version (`nifty50_processed/`)
- ✅ All files have **753 columns** (matches sample exactly)
- ✅ All files have **Future data** (FUT_I, FUT_II, FUT_III)
- ⚠️ Only 10 files have **Option data** (because of strike price mismatch)

**Examples:**
- `TCS_2025-10-31.feather` - Has futures + options (10% data)
- `RELIANCE_2025-10-31.feather` - Has futures only (1.8% data)

### Dynamic Version (`nifty50_processed_dynamic/`)
- ✅ All files have **variable columns** (based on actual data)
- ✅ All files have **Future data**
- ✅ All files have **Option data** (fully populated)

**Examples:**
- `TCS_2025-10-31.feather` - 435 columns (36% data)
- `RELIANCE_2025-10-31.feather` - 477 columns (39% data)

---

## Data Quality Summary

| Metric | Strict Version | Dynamic Version |
|--------|---------------|-----------------|
| Files Created | 50/50 ✅ | 50/50 ✅ |
| Column Count | Always 753 | 171-501 (varies) |
| Future Data | 100% ✅ | 100% ✅ |
| Option Data | 20% (10 stocks) | 100% ✅ |
| Avg Data Density | 1-10% | 20-55% |

---

## Common Questions

### Q: Why are some files mostly empty?
**A:** The strict version uses fixed columns from the sample file. If a stock's price is different, the data won't fit.

### Q: Which version should I use?
**A:** 
- Use **Strict** if you need exact column matching (for the assignment)
- Use **Dynamic** if you need all the data (for actual analysis)

### Q: Where are the CSV files?
**A:** Run `python3 2.py` to create CSV files in `nifty50_csv/` folder.

### Q: How do I check if files are correct?
**A:** Run: `python3 validate_submission.py nifty50_processed strict`

---

## File Structure

```
Multyfi/
├── process_to_feather.py          # Main script (strict)
├── full_analysis.py               # Alternative (dynamic)
├── README.md                      # This file
├── GFDLNFO_BACKADJUSTED_31102025.csv  # Input file
├── ACC_2024-05-07.feather         # Sample file (template)
├── nifty50_processed/             # Output (strict)
└── nifty50_processed_dynamic/     # Output (dynamic)
```

---

## Dependencies

```
Python 3.7+
pandas
pyarrow
numpy
```

Install with:
```bash
pip install pandas pyarrow numpy
```

---

## Author Notes

This project solves the NIFTY-50 data processing task by:
1. Reading the master CSV efficiently
2. Parsing derivative ticker formats correctly
3. Converting long format to wide format
4. Matching the sample file structure exactly
5. Handling both futures and options data

The main challenge was handling the strike price mismatch between the sample file (ACC, 2024) and the actual data (2025, different stocks, different prices). I solved this with two approaches: problem (exact match) full (data-first).

---

## License

This project is for educational purposes.

---

## Support

If you need help:
1. Check the README files 
2. Run
3. Check the output logs for error messages
