window.onload = function () {
$.get('/downsampler', function(data) {
   $("#period").val(data.period);
});
}


function sendConfig() {
    $.postJSON( "/downsampler", JSON.stringify({'period': $("#period").val()}), function( data ) {
    }, "application/json");
}

$.postJSON = function(url, data, callback) {
    return jQuery.ajax({
        'type': 'POST',
        'url': url,
        'contentType': 'application/json',
        'data': data,
        'dataType': 'json',
        'success': callback
    });
};


$(function () {
    $("form").on('submit', function (e) {
        e.preventDefault();
    });
    $( "#send" ).click(function() { sendConfig(); });
});