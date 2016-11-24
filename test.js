var async = require('async');
var levelup = require('level');
//var inputDb = levelup('./my-input');
var inputDb = levelup('./000extractBLChargesFromARP20160930.140046.328');
var outputDb = levelup('./ARP-output');
var length = 5000000;
function convertToString(number,formatLength){
    var result=''+number;
    var initLength=result.length;
    if(initLength<10){
        if(!formatLength){
            formatLength=10;
        }
        for(var i=initLength;i<formatLength;i++){
            result='0'+result;
        }
    }
    return result;
}



function initData(callback){
    /*if unordered data exists, skip this step*/
    console.log('init data...');
    var key=0;
    function loop(){
        var data={
            id:  Math.floor(Math.random() * length),
            context: 'test'
        };
        inputDb.put(convertToString(key),JSON.stringify(data),function(err){
            inputDb.get(convertToString(key), function(err, value){
                key++;
                if(key<length){
                    loop();
                }
                else {
                    console.log('init data done');
                    callback();
                }
            });
        });
    }
    loop();
}

function sort(callback){
    console.log('sorting...');
    var begin = new Date().getTime();
    externalSort.externalSort(
        inputDb,
        outputDb,
        function(a,b){
            return parseInt(a[0]) - parseInt(b[0]);//compare function+++++++
        },
        function(totalSize){
            console.log('Time cost: '+ (new Date().getTime() - begin)+' ms');
            console.log('totalSize: '+totalSize);
            length=totalSize;
            callback();
        }
    );
}

function validation(callback) {
    console.log('validating...');
    var lastData=0;
    var currentData=0;
    var key=0;
    function loop(){
        outputDb.get(convertToString(key),function(err, value){
            if(key>=length) {
                callback(null,true);
            }
            else if(err) {
                callback(null,false);
            }
            else {
                currentData = (JSON.parse(value))[0];
                if(currentData>=lastData){
                    lastData=currentData;
                    key++;
                    loop();
                }
                else {
                    callback(null,false);
                }
            }
        })
    }
    loop();
}

var externalSort = require('external-sort');
async.series([/*initData,*/sort,validation], function (err, result) {
    console.log('validation: '+result/*[2]*/);
});
