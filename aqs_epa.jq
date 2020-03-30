.Data | 
    (.[0] | keys_unsorted) as $keys |
    $keys, map([.[ $keys[] ]])[] | 
    @csv

