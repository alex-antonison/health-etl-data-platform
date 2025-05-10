-- Create base table for AppResult
CREATE TABLE app_results (
    id serial primary key,
    polymorphic_type VARCHAR(255) NOT NULL,
    created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content_slug VARCHAR(255) NOT NULL
);
-- Create table for DateTimeAppResult
CREATE TABLE datetime_app_results (
    id serial primary key,
    app_result_id INTEGER NOT NULL references app_results(id)
    ON
    DELETE
        CASCADE,
        VALUE TIMESTAMP NOT NULL
);
-- Create table for IntegerAppResult
CREATE TABLE integer_app_results (
    id serial primary key,
    app_result_id INTEGER NOT NULL references app_results(id)
    ON
    DELETE
        CASCADE,
        VALUE INTEGER NOT NULL
);
-- Create table for RangeAppResult
CREATE TABLE range_app_results (
    id serial primary key,
    app_result_id INTEGER NOT NULL references app_results(id)
    ON
    DELETE
        CASCADE,
        from_value INTEGER NOT NULL,
        to_value INTEGER NOT NULL,
        constraint valid_range CHECK (
            from_value <= to_value
        )
);
-- Create indexes for better query performance
CREATE INDEX idx_app_results_content
ON app_results(content_slug);
CREATE INDEX idx_app_results_created
ON app_results(created_time);
-- Create function to update modified_time
CREATE
OR REPLACE FUNCTION update_modified_time() returns TRIGGER AS $$
BEGIN
    NEW.modified_time = CURRENT_TIMESTAMP;
RETURN NEW;
END;$$ LANGUAGE 'plpgsql';
-- Create trigger to automatically update modified_time
CREATE TRIGGER update_app_results_modified_time before
UPDATE
    ON app_results FOR each ROW EXECUTE FUNCTION update_modified_time();
