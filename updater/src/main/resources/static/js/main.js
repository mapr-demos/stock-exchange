window.onload = function () {
$.get('/profile', function(data) {
  $.each(data, function(index, value){
    $("#data").append('<a href="/profile.html?name=' + value.name + '&fullName= ' + value.fullName + '" class="list-group-item">' + value.fullName + '</a>');
  });
});
}
