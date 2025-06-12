{#
    This macro retrieves the payment type based on the provided payment method.
#}


{% macro get_payment_type_description(payment_type) -%}
    case cast({{ payment_type }} as int)
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Empty'
    end
{%- endmacro %}