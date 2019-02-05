window.onload = function () {
        var sock = new SockJS('/statistic-sockets');

        sock.onopen = function() {
          console.log('web socket opened');
          var message = {
              type: "SUBSCRIBE",
              topic: "ALL"
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

    function handleSystemInfo(content) {
        if(content.status == 'SUBSCRIBE')
            console.log("subscribed to " + content.topicName);
        else if(content.status == 'SUBSCRIBE')
            console.log("unsubscribed from " + content.topicName);
    }

    function updateData(content) {
        if(content.uiId != null) {
            var subscriber = $("#" + content.uiId).text();
            if(jQuery.isEmptyObject(subscriber))
                $("#subscribers-data").append('<a id="' + content.uiId + '" class="list-group-item">' + '<b>' + content.uiId + '</b> <span class="badge">' + content.subscribersAmount + '</span></a>');
            else
                $("#" + content.uiId).html('<b>' + content.uiId + '</b> <span class="badge">' + content.subscribersAmount + '</span>');
        }

        if(content.symbol != null) {
            updateRate(content.symbol, content.incomingRate);
        }

    }

    function updateRate(name, value) {
        var subscriber = $("#" + name).text();
        if(jQuery.isEmptyObject(subscriber))
            $("#rates-data-table").append('<tr id = "' + name + '"><td>' + name.replace("_", " ") + '</td><td class="rate">' + value.toFixed(4) + '</td>');
        else
            $("#" + name + " td.rate").html(value.toFixed(4));
    }

}