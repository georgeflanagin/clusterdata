watts()
{
    pushd ~installer/clusterdata >/dev/null
    if [ -z $1 ]; then
        sqlite3 power.db "select sum(watts) from facts where t = (select max(t) from facts) and point = 't';"
    elif [ "$1" == "all" ]; then
        sqlite3 power.db "select node, watts from facts where \
            t = (select max(t) from facts) and point = 't' order by node;"
    else
        sqlite3 power.db "select watts from facts where \
            t = (select max(t) from facts) and point = 't' and node = $1;"
    fi
    popd >/dev/null
        
}
