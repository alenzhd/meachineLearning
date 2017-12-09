basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3

#生成待爬取商品ID的文件，并且推送到接口目录
python $basepath/dmp_spider_goods_getid.py $provider $province $dates

#监控爬虫结果的接口目录，如果有数据，则更新商品库
python $basepath/dmp_spider_goods_getinfo.py  $provider $province  $dates

#将商品实时爬取的商品信息，更新到商品库
python $basepath/dmp_realtime_goods_add.py $provider $province  $dates
