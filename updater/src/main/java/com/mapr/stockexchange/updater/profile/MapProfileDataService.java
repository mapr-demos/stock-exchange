package com.mapr.stockexchange.updater.profile;

import com.mapr.stockexchange.updater.entitiy.Profile;
import com.mapr.stockexchange.updater.entitiy.Stock;
import com.mapr.stockexchange.updater.profile.exception.RuntimeProfileNotFoundException;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;

@Service
public class MapProfileDataService implements ProfileDataService {

    private final Map<String, Profile> profilesAndStocks = new HashMap<>();

    @PostConstruct
    public void init() {
        addData();
    }

    @Override
    public Collection<Profile> getProfiles() {
        return profilesAndStocks.values();
    }

    @Override
    public List<Stock> getStocksFromProfile(String profileName) {
        List<Stock> list = profilesAndStocks.get(profileName).getStocks();

        if(list != null)
            return list;
        else
            throw new RuntimeProfileNotFoundException(String.format("No profile with name '%s'", profileName));
    }

    private void addData() {
        List<Stock> list = getDJIAList();

        profilesAndStocks.put("djia", new Profile("djia", "Dow Jones Industrial Average", list));
        profilesAndStocks.put("profile2", new Profile("profile2", "Profile 2", list));
        profilesAndStocks.put("profile3", new Profile("profile3", "Profile 3", list));
    }

    private List<Stock> getDJIAList() {
        List<Stock> list = new LinkedList<>();

        list.add(new Stock("MMM", ""));
        list.add(new Stock("AXP", ""));
        list.add(new Stock("AAPL", ""));
        list.add(new Stock("BA", ""));
        list.add(new Stock("CAT", ""));
        list.add(new Stock("CVX", ""));
        list.add(new Stock("CSCO", ""));
        list.add(new Stock("KO", ""));
        list.add(new Stock("DWDP", ""));
        list.add(new Stock("XOM", ""));
        list.add(new Stock("GS", ""));
        list.add(new Stock("HD", ""));
        list.add(new Stock("IBM", ""));
        list.add(new Stock("INTC", ""));
        list.add(new Stock("JNJ", ""));
        list.add(new Stock("JPM", ""));
        list.add(new Stock("MCD", ""));
        list.add(new Stock("MRK", ""));
        list.add(new Stock("MSFT", ""));
        list.add(new Stock("NKE", ""));
        list.add(new Stock("PFE", ""));
        list.add(new Stock("PG", ""));
        list.add(new Stock("TRV", ""));
        list.add(new Stock("UNH", ""));
        list.add(new Stock("UTX", ""));
        list.add(new Stock("VZ", ""));
        list.add(new Stock("V", ""));
        list.add(new Stock("WMT", ""));
        list.add(new Stock("WBA", ""));
        list.add(new Stock("DIS", ""));

        return list;
    }

}
