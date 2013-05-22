google.maps.visualRefresh = true;

var map;
// Start position
var startMarker;
// End position
var endMarker;
// Realtime bus locations
var busPositions = {};
// true to show bus locations
var showPositions = false;
// current bus routes, key is tripID, value is (tripID, startDate, stopID)
var busTrips = {};
// Google Maps direction service
var dirServ = new google.maps.DirectionsService();

function initialize() {
  var mapOptions = {
    zoom: 13,
    center: new google.maps.LatLng(42.373637,-71.109352), // Cambridge
    mapTypeId: google.maps.MapTypeId.ROADMAP,
    streetViewControl: false
  };
  map = new google.maps.Map(document.getElementById('map-canvas'),
      mapOptions);

  setupMarkers();

  // Create the DIV to hold the position toggle button
  placePositionBtn(map);

  google.maps.event.addListener(map, 'click', function(event) {
    placeStartMarker(event.latLng);
  });
}

function setupMarkers() {
  startMarker = new google.maps.Marker({
      position: new google.maps.LatLng(42.374588,-71.117291), // Harvard
      map: map,
      title: 'Start from here',
      animation: google.maps.Animation.DROP,
      draggable: true
  });
  google.maps.event.addListener(startMarker, 'dragend', function() {
    console.log('requesting transit by moving the start marker');
    requestTransit(startMarker.getPosition(), endMarker.getPosition());
  });

  endMarker = new google.maps.Marker({
      position: new google.maps.LatLng(42.359686,-71.094289), // MIT
      map: map,
      title: 'End here',
      animation: google.maps.Animation.DROP,
      draggable: true
  });
  google.maps.event.addListener(endMarker, 'dragend', function() {
    console.log('requesting transit by moving the end marker');
    requestTransit(startMarker.getPosition(), endMarker.getPosition());
  });
}

function placeStartMarker(location) {
  startMarker.setPosition(location);

  console.log('requesting transit');
  requestTransit(location, endMarker.getPosition());

  map.panTo(location);
}

function getTripPosition(trip) {
  query('trip/position/', {'tripId' : trip.id, 'startDate': trip.startDate},
        function(data) {
    updatePositions(data);

    if (trip.id in busTrips) {
      // Update the position 30 seconds later
      window.setTimeout(function() {getTripPosition(trip);}, 30000);
    }
  });
}

function updatePositions(positions) {
  positions.forEach(function(element) {
    var marker = busPositions[element['tripId']];
    if (!marker) {
      marker = createPositionMarker(element);
      busPositions[element['tripId']] = marker;
    }

    marker.setPosition(new google.maps.LatLng(element['lat'], element['lng']));
  });
}

function createPositionMarker(position) {
  return new google.maps.Marker({
      position: new google.maps.LatLng(position['lat'], position['lng']),
      map: showPositions ? map : null,
      title: position['name'],
      icon: 'https://google-maps-icons.googlecode.com/files/bus.png'
  });
}

function togglePositionMarkerVisibility(visible) {
  for (var key in busPositions) {
    if (visible) {
      busPositions[key].setMap(map);
    } else {
      busPositions[key].setMap(null);
    }
  }
}

function requestTransit(startLocation, endLocation) {
  var dirReq = {
    origin: startLocation,
    destination: endLocation,
    provideRouteAlternatives: true,
    travelMode: google.maps.TravelMode.TRANSIT
  };

  reset();

  dirServ.route(dirReq, function(result, status) {
    if (status === 'OK') {
      var busRoutes = findBusRoute(result);
      for (var route in busRoutes) {
        findNextTrip(route, busRoutes[route].name);
      }
    } else {
      printMsg('No valid results: ' + status, true);
      console.warn(result);
    }
  });
}

function findBusRoute(directions) {
  var busRoutes = {};

  for (var routeIndex = 0;
       routeIndex < directions['routes'].length;
       routeIndex++) {
    var busStep = findFirstBusStep(directions['routes'][routeIndex]);
    if (busStep) {
      var stopInfo = extractStopInfo(busStep);
      var busInfo = extractBusInfo(busStep);

      busRoutes[busInfo] = stopInfo;
    } else {
      console.log('No eligible bus routes');
      console.log(directions['routes'][routeIndex]);
    }
  }

  console.log(busRoutes);

  return busRoutes;
}

// Returns a bus step or null
function findFirstBusStep(route) {
  // only care about the first leg
  var steps = route['legs'][0]['steps'];

  // only care about the first non-walking step
  var firstBusStep;
  for (var stepIndex = 0; stepIndex < steps.length; stepIndex++) {
    if (steps[stepIndex]['travel_mode'] === 'WALKING') {
      continue;
    }

    if (steps[stepIndex]['travel_mode'] === 'TRANSIT' &&
        steps[stepIndex]['transit']['line']['vehicle']['type'] === 'BUS' &&
        steps[stepIndex]['transit']['line']['agencies'][0]['name'] === 'MBTA') {
      firstBusStep = steps[stepIndex];
      break;
    } else {
      break;
    }
  }

  return firstBusStep;
}

function extractStopInfo(step) {
  if (step) {
    return step['transit']['departure_stop'];
  } else {
    return null;
  }
}

function extractBusInfo(step) {
  if (step) {
    return step['transit']['line']['short_name'];
  } else {
    return null;
  }
}

function placePositionBtn(map) {
  var positionControlDiv = document.createElement('div');
  var positionControl = new createPositionBtn(positionControlDiv, map);
  positionControlDiv.index = 1;
  map.controls[google.maps.ControlPosition.TOP_RIGHT].push(positionControlDiv);
}

function createPositionBtn(controlDiv, map) {
  // Set CSS styles for the DIV containing the control
  // Setting padding to 5 px will offset the control
  // from the edge of the map.
  controlDiv.style.padding = '5px';

  // Set CSS for the control border.
  var controlUI = document.createElement('div');
  controlUI.className = 'btn btn-small';
  // Set default text
  togglePositionBtn(controlUI);
  controlDiv.appendChild(controlUI);

  // Setup the click event listeners: simply set the map to Chicago.
  google.maps.event.addDomListener(controlUI, 'click', function() {
    showPositions = togglePositionBtn(controlUI);
    togglePositionMarkerVisibility(showPositions);
  });
}

/**
 * Returns true to show positions, false to hide them.
 */
function togglePositionBtn(btn) {
  if (btn.innerHTML === 'Show Positions') {
    btn.innerHTML = 'Hide Positions';
    btn.title = 'Hide real-time bus positions';
    return true;
  } else {
    btn.innerHTML = 'Show Positions';
    btn.title = 'Show real-time bus positions';
    return false;
  }
}

function clearRouteTable() {
  $('#routes > tbody:last').empty();
}

function addRouteToTable(route, trip, stopId, eta) {
  var subscribe = '<form class="form-inline">' +
        '<input type="hidden" name="tripId" value="' + trip.id + '">' +
        '<input type="hidden" name="startDate" value="' + trip.startDate + '">' +
        '<input type="hidden" name="stopId" value="' + stopId + '">' +
        '<input class="input-small delayMin" name="delay" type="number" placeholder="minutes">' +
        '</form>';
  var row = '<tr><td>' + route + '</td><td>' + eta + '</td><td>' + subscribe + '</td></tr>';
  $('#routes > tbody:last').append(row);

  // Use Ajax to submit the form so the page doesn't refresh
  $('.delayMin').keydown(function() {
    if (event.keyCode == 13) {
      query('trip/subscribe/', $(this.form).serialize(), function(data) {
        printMsg(data, false);
      });
      return false;
    } else {
      return true;
    }
  });
}

function subscribeToTrip(trip, stopId, delay) {
  query('trip/subscribe/',
        {'tripId' : trip.id,
         'startDate': trip.startDate,
         'stopId': stopId,
         'delay': delay},
        function(data) {
    // TODO: what does it return
  });
}

function findNextTrip(routeId, stopName) {
  query('route/', {'routeId' : routeId, 'stopName': stopName},
        function(data) {
    console.log('Found next trip ' + data);
    var trip = {'id': data[0], 'startDate': data[1]};
    busTrips[trip.id] = data;
    addRouteToTable(routeId, trip, data[3], data[2]);

    // Start watching the position of the trip
    getTripPosition(trip);
  });
}

function printMsg(msg, warn) {
  var msgContainer = $('#msg-pane');
  msgContainer.text(msg);

  if (warn) {
    msgContainer.addClass('text-error');
  } else {
    msgContainer.removeClass('text-error');
  }
}

function reset() {
  // Reset route table and trips
  clearRouteTable();
  busTrips = {};
}

function query(endpoint, params, handler) {
  var apiURL = '/api/' + endpoint;
  $.getJSON(apiURL, params)
   .done(function(data) {
     handler(data);
   })
   .fail(function(data) {
     printMsg(data.statusText, true);
   });
}

/**
 * Check if the returned volt response contains error. If yes, return the status string.
 */
function checkErr(result) {
  if (result['status'] != 1) {
    return result['statusstring'];
  } else {
    return null;
  }
}

/**
 * Retrieve the result VoltTables
 */
function getResultTable(result) {
  return result['results'];
}

google.maps.event.addDomListener(window, 'load', initialize);
