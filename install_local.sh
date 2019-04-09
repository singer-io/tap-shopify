#! /bin/bash
get_value_from_file() {
    key=$1
    file=$2

    val=( $(cat $file | grep -Eo "${key}=.{1,50}" | sed "s/^${key}=\"\(.*\)\",$/\1/") )
    echo $val
}



GREEN='\033[0;32m'
NC='\033[0m' # No Color
YELLOW='\033[0;33m'
RED='\033[0;31m'



lib_version=( $(get_value_from_file 'version' setup.py) )
lib_name=( $(get_value_from_file 'name' setup.py) )



python3 setup.py sdist bdist_wheel
pip uninstall --y $lib_name
if pip install ./dist/${lib_name}-${lib_version}.tar.gz ; then
    rm -rf ./build
    rm -rf ./dist
    rm -rf ./*.egg-info
    echo 
    echo -e "${GREEN}##########################################################${NC}"
    echo -e "${GREEN}  Successfully Installed: ${YELLOW}${lib_name}-${lib_version}${NC}"
    echo -e "${GREEN}##########################################################${NC}"
    echo 
else
    echo 
    echo -e "${RED}##########################################################${NC}"
    echo -e "${RED}  Failed to Install: ${YELLOW}${lib_name}-${lib_version}${NC}"
    echo -e "${RED}##########################################################${NC}"
    echo 
fi

