var getUrlParameter = function getUrlParameter(sParam) {
    var sPageURL = window.location.search.substring(1),
        sURLVariables = sPageURL.split('&'),
        sParameterName,
        i;

    for (i = 0; i < sURLVariables.length; i++) {
        sParameterName = sURLVariables[i].split('=');

        if (sParameterName[0] === sParam) {
            return sParameterName[1] === undefined ? true : decodeURIComponent(sParameterName[1]);
        }
    }
};



window.onload = function () {
    $("#profile-name").text(getUrlParameter('fullName'));

     $.get('/profile/'+ getUrlParameter('name'), function(data) {
        $.each(data, function(index, value){
            $("#data-table").append('<tr id = "' + value.name + '"><td>' + value.name.replace("_", " ") + '</td><td class="price"></td><td class="change"></td><td class="percent-change"></td><td class="volume"></td><td class="time"></td></tr>')
        });
        connect(getUrlParameter('name'));
    });

    function connect(profile) {
        var sock = new SockJS('/profiles-sockets');

        sock.onopen = function() {
          console.log('web socket opened');
          var message = {
              type: "SUBSCRIBE",
              topic: profile
            };

            // Send it now
            sock.send(JSON.stringify(message));
        };

        sock.onclose = function() {
          console.log('web socket closed close');
        };

        sock.onmessage = function(e) {
          var content = jQuery.parseJSON(e.data)
          if(content.topic == 'server')
            handleSystemInfo(content);
          else
            updateData(content);
        };
    }

    function handleSystemInfo(content) {
        if(content.status == 'SUBSCRIBE')
            console.log("subscribed to " + content.topicName);
        else if(content.status == 'SUBSCRIBE')
            console.log("unsubscribed from " + content.topicName);
    }

    function updateData(content) {
        var oldPrice = $("#" + content.data.name + " td.price").html();

        if(oldPrice == '')
            oldPrice = content.data.price;

        var change = content.data.price - oldPrice;

        $("#" + content.data.name + " td.price").html(content.data.price);

        if(change > 0) {
            $("#" + content.data.name + " td.change").html("+" + change.toFixed(2));
            $("#" + content.data.name + " td.percent-change").html("+" + (100* change / oldPrice).toFixed(2) + "%");
            $("#" + content.data.name + " td.change").css('color', '#009900');
            $("#" + content.data.name + " td.percent-change").css('color', '#009900');
        } else if(change < 0) {
            $("#" + content.data.name + " td.change").html(change.toFixed(2));
            $("#" + content.data.name + " td.percent-change").html((100* change / oldPrice).toFixed(2) + "%");
            $("#" + content.data.name + " td.change").css('color', '#ff0000');
            $("#" + content.data.name + " td.percent-change").css('color', '#ff0000');
        }

        $("#" + content.data.name + " td.volume").html(content.data.volume);
        $("#" + content.data.name + " td.time").html($.format.date(new Date(content.data.date), "HH:mm:ss"));
    }
}