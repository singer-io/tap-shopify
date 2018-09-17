echo '{"type": "SCHEMA",  "stream": "first_stream",  "schema": {},  "key_properties": ["key_prop1"],  "bookmark_properties": ["bookmark_prop"]}'
echo '{"type": "SCHEMA",  "stream": "second_stream",  "schema": {},  "key_properties": ["key_prop2"],  "bookmark_properties": ["bookmark_prop"]}';

while sleep 1
do
    echo '{"type": "RECORD", "stream": "first_stream", "record": {"key_prop1": 1, "bookmark_prop": "Chris"}}'
    echo '{"type": "RECORD", "stream": "second_stream", "record": {"key_prop2": 1, "bookmark_prop": "Chris"}}'
    if (( (SECONDS % 10) == 0 ))
    then
        echo '{"type": "STATE", "value": {"first_stream": '"$SECONDS"', "second_stream": '"$(( SECONDS + 10 ))"'}}'
    fi
done
