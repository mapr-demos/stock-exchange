package com.mapr.stockexchange.updater.profile;

import com.mapr.stockexchange.updater.entitiy.Profile;
import com.mapr.stockexchange.updater.entitiy.Stock;

import java.util.Collection;

public interface ProfileDataService {
    Collection<Profile> getProfiles();

    Collection<Stock> getStocksFromProfile(String profileName);

}
