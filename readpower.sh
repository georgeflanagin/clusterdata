function readpower
{
    if [ -z $1 ]; then
        echo "Using defaults"
        python readpower.py
    elif [ $1 == "-?" ]; then
        python readpower.py --help
    else
        python readpower.py $@
    fi
}

readpower -?
