{% macro test_field(model, column_name, special_type='', visibility_type='normal') -%}

select sum(count) as count from (
  
  select count(*) as count where '{{ special_type }}' not in (
      'type/AvatarURL',
      'type/Category',
      'type/City',
      'type/Country',
      'type/Currency',
      'type/Description',
      'type/Email',
      'type/Enum',
      'type/ImageURL',
      'type/SerializedJSON',
      'type/Latitude',
      'type/Longitude',
      'type/Number',
      'type/State',
      'type/URL',
      'type/ZipCode',
      'type/Quantity',
      'type/Income',
      'type/Discount',
      'type/CreationTimestamp',
      'type/CreationTime',
      'type/CreationDate',
      'type/CancelationTimestamp',
      'type/CancelationTime',
      'type/CancelationDate',
      'type/DeletionTimestamp',
      'type/DeletionTime',
      'type/DeletionDate',
      'type/Product',
      'type/User',
      'type/Source',
      'type/Price',
      'type/JoinTimestamp',
      'type/JoinTime',
      'type/JoinDate',
      'type/Share',
      'type/Owner',
      'type/Company',
      'type/Subscription',
      'type/Score',
      'type/Title',
      'type/Comment',
      'type/Cost',
      'type/GrossMargin',
      'type/Birthdate',
      ''
  )
  
  union

  select count(*) as count where '{{ visibility_type }}' not in (
      'details-only',
      'hidden',
      'normal',
      'retired',
      'sensitive'
  )
  
);

{%- endmacro %}
