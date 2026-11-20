-- Inject a source change only after the first execution has captured its immutable batch.
UPDATE `{{ project }}.{{ dataset }}.{{ base }}`
SET Customer_Number = 'invalid_after_capture'
WHERE Line_Type = 'INV';
