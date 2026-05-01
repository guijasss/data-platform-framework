# Flow

* Two modes of reading:
    - FULL LOAD: reads the entire source table
    - INCREMENTAL: reads data where processed_at is greater than a watermark

* Verifications:
    - check if source table exists
    - check if source table has updates
        - for this verification, read from a yaml config file if user wants this feature

* write modular code, with a clean and logical flow, using DRY and KISS
