package com.mapr.stockexchange.updater.controller;

import com.mapr.stockexchange.updater.entitiy.Profile;
import com.mapr.stockexchange.updater.entitiy.Stock;
import com.mapr.stockexchange.updater.profile.ProfileDataService;
import com.mapr.stockexchange.updater.profile.exception.RuntimeProfileNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/profile", produces = "application/json", method = RequestMethod.GET)
public class ProfilesController {
    private final ProfileDataService dataService;

    @RequestMapping(value = "")
    public Collection<Profile> getProfiles() {
        log.info("Get request for '/profile'");
        return dataService.getProfiles();
    }

    @RequestMapping(value = "/{name}")
    public Collection<Stock> getStocksByProfile(@PathVariable String name) {
        log.info("Get request for '/profile/{}'", name);
        return dataService.getStocksFromProfile(name);
    }

    @ExceptionHandler(RuntimeProfileNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String handleResourceNotFoundException(Exception e) {
        return e.getMessage();
    }

}
