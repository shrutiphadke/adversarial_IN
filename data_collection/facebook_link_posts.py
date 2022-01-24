import pandas as pd
import json
from collections import defaultdict
from collections import Counter
import time 
from six.moves import urllib
import wget
import logging
logging.basicConfig(filename='facebook_lp_jan20.log',level=logging.DEBUG)
import pickle as pkl
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
import glob

month_conv = {1:'01', 2:'02', 3:'03', 4:'04', 5:'05', 6:'06', 7:'07', 8:'08', 9:'09', 10:'10', 11:'11', 12:'12'}
higher_date = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}

domains = pd.read_csv("/home/phadke/ONR/ONR/lite_data/domains_for_facebook.csv", header=0)

done_fnames = [x.replace('/data/shruti/ONR/big_data/facebook_linkposts/', "") for x in glob.glob("/data/shruti/ONR/big_data/facebook_linkposts/*")]

domains = domains.loc[~domains['domain'].isin(done_fnames)]

links = domains.loc[domains['is_news']==1]['domain'].tolist()

print(links)

unfinished = ['telegraphindia.com', 'kanaknews.com', 'samajalive.in', 'enewsinsight.com', 'indiatv.in', 'firstindianews.com', 'maharashtratimes.com', 'organiser.org', 'tv9bharatvarsh.com', 'punjabkesari.com', 'nationnews.in', 'news24online.com', 'nationalheraldindia.com', 'lagatar.in', 'newswing.com', 'businesstoday.in', 'cnbctv18.com', 'asianetnews.com', 'theweek.in', 'indiatvnews.com', 'policenama.com', 'aninews.in', 'sarkarnama.in','barandbench.com', 'economictimes.com', 'punjabkesari.in', 'trunicle.com', 'humsamvet.com', 'newsnationtv.com', 'satyahindi.com', 'thewirehindi.com', 'navodayatimes.in', 'newslaundry.com', 'agniban.com', 'upuklive.com', 'dailyo.in', 'thenewsagency.in', 'newsnasha.com', 'prabhatkhabar.com', 'sharpbharat.com', 'biharjharkhandnewsnetwork.com', 'publictv.in', 'vijaykarnataka.com', 'vijayapatha.in', 'pratikshana.com', 'thenewsminute.com', 'newsfirstlive.com', 'prokerala.com', 'thehansindia.com', 'ibtimes.co.in', 'thegoan.net', 'mpcnews.in', 'aappune.org', 'mpbreakingnews.in', 'navshaktinews.com', 'commondispatch.com', 'news4nation.com', 'navjivanindia.com', 'newsclick.in', 'kashishnews.com', 'digitalnewslive.com', 'firstbihar.com', 'jansagar.in', 'bihardootnews.com', 'newstrack.com', 'indiadarpanlive.com', 'lalluram.com', 'ibc24.in', 'naidunia.com', 'glibs.in', 'sanjeevnitoday.com', 'dkoding.in', 'mumbaitak.in', 'thestatesman.com', 'goanewshub.com', 'heraldgoa.in', 'dainikgomantak.com', 'goanvartalive.com', 'uniindia.com', 'tarunbharat.com', 'prudentmedia.in', 'tv9gujarati.com', 'sandesh.com', 'trishulnews.com', 'meranews.com', 'hwnews.in', 'sentinelassam.com', 'prabhasakshi.com', 'newstracklive.com', 'siasat.com', 'dailypioneer.com', 'haribhoomi.com', 'mid-day.com', 'newsonair.com', 'thehindubusinessline.com', 'zeebiz.com', 'daijiworld.com', 'outlookhindi.com', 'businessworld.in', 'socialnews.xyz', 'businessinsider.in', 'theshillongtimes.com', 'telanganatoday.com', 'nayaindia.com', 'hindikhabar.com', 'specialcoveragenews.in', 'rajyasameeksha.com', 'nyoooz.com', '5dariyanews.com', 'nagpurtoday.in', 'pardaphash.com', 'carandbike.com', 'indialegallive.com', 'inc42.com', 'punjabnewsexpress.com', 'mumbailive.com', 'forbesindia.com', 'thedailyguardian.com', 'kannadaprabha.com', 'mysoorunews.com', 'inshorts.com', 'tarunmitra.in', 'uttarpradesh.org', 'divyabhaskar.co.in', 'maxmaharashtra.com', 'krushirang.com', 'thelallantop.com', 'lokshahi.live', 'hellomumbainews.com', 'udayavani.com', 'prasthutha.com', 'eurasiantimes.com', 'sakshi.com', 'dailythanthi.com', 'news7tamil.live', 'newsroompost.com', 'orissapost.com', 'punekarnews.in', 'thebetterindia.com', 'janjwar.com', 'dailyexcelsior.com', 'knskashmir.com', 'newsbharati.com', 'palpalindia.com', 'japantimes.co.jp', 'manoramaonline.com', 'ptcnews.tv', 'twocircles.net', 'jantantratv.com', 'kannadamedia.com', 'gujaratexclusive.in', 'connectgujarat.com', 'sangbadpratidin.in', 'downtoearth.org.in', 'eisamay.com', 'rastrasamvad.com', 'youthkiawaaz.com', 'newsx.com', 'sify.com', 'catchnews.com', 'greaterkashmir.com', 'forceindia.net', 'thodkyaat.com', 'panchjanya.com', 'mymahanagar.com', 'dishadaily.com', 'odishareporter.in', 'ichowk.in', 'enavabharat.com', 'onmanorama.com', 'janmabhumi.in', 'newsbred.com', 'ritamdigital.org', 'realreport.in', 'manoramanews.com', 'mediavigil.com', 'sudarshannews.in', 'npg.news', 'agrowon.com', 'molitics.in', 'khabaradda.in', 'topchand.com', 'sabrangindia.in', 'dailychhattisgarh.com', 'newsnetworks.co.in', 'khaskhabar.com', 'polimernews.com', 'vikatan.com', 'kalaignarseithigal.com', 'newsdanka.com','sandeshnews.tv', 'jharkhandsandesh.in', 'news9live.com', 'saamtv.com', 'newsnetwork24x7.com', 'keralakaumudi.com', 'newslivetv.com', 'banglahunt.com', 'punchnamu.com', 'deccanchronicle.com', 'tricitytoday.com', 'eastmojo.com', 'asomiyapratidin.in', 'nenow.in', 'uptak.in', 'livegorakhpur.com', 'jantaserishta.com', 'khojinews.co.in', 'chetnamanch.com', 'jantakiawaz.org', 'univarta.com', 'deshbandhu.co.in', 'beforeprint.in', 'jaihindtv.in', 'mynation.com', 'eenadu.net', 'odishabytes.com', 'argusnews.in', 'opoyi.com', 'orissadiary.com', 'gaurilankeshnews.com', 'swadeshnews.in', 'statetodaytv.com', 'marathiebatmya.com', 'jaimaharashtranews.com', 'mtmobile.in', 'thefocusindia.com', 'indiainfoline.com', 'newskranti.com', 'kathir.news', 'newsmeter.in', 'samayam.com', 'tolivelugu.com', 'onlinenewsindia.co.in', 'thenationalbulletin.in', 'doonhorizon.in', 'akilanews.com', 'dainikbharat24.com', 'pudhari.news', 'hindusthanpost.com', 'mirrormaharashtra.in', 'guwahatiplus.com', 'bhaskarlive.in', 'kashmirobserver.net', 'newindian.in', 'epapervijayavani.in', 'dinamani.com', 'thedispatch.in', 'newsmobile.in', 'lawbeat.in', 'satyaprahari.com', 'fortuneindia.com', 'newsijt.com', 'shasakprashasak.com', 'chitralekha.com', 'rashtriyasahara.com', 'clarionindia.net', 'medicaldialogues.in', 'indiaeducationdiary.in', 'canindia.com', 'kashmirlife.net', 'udupitimes.com', 'palpalnews.in', 'thebridge.in', 'inventiva.co.in', 'navarashtra.com', 'circle.page', 'dainiktribuneonline.com', 'jammulinksnews.com', 'newsncr.com', 'avatarnews.in', 'anewsoffice.com', 'indianewsjaihind.com', 'ahmedabadmirror.com', 'arunchol.com', 'khabarchalisa.com', 'jhansitimes.com', 'dinakaran.com', 'andhrajyothy.com', 'v6velugu.com', 'qaumiawaz.com', 'livevns.news', 'cgwall.com', 'madhyamam.com', 'bhadas4media.com', 'swadeshtoday.com', 'oredesam.in', 'divyahimachal.com', 'dtnext.in', 'ibctamilnadu.com', 'patrikai.com', 'malaimurasu.com', 'ifp.co.in', 'thenews21.com', 'politicalmaharashtra.in', 'bansalnews.com', 'medianama.com', 'gonewsindia.com', 'urbantransportnews.com', 'therealkashmir.com', 'thefauxy.com', 'munaadi.com', 'threesocieties.com', 'himachalabhiabhi.com', 'khabarchhe.com', 'theleaflet.in', 'agnialok.com', 'modipara.com', 'assamtribune.com', 'hastakshep.com', 'prsindia.org', 'raftaar.in', 'breakingtube.com', 'suryodaybharat.com', 'insidene.com', 'thecitizenmirror.in', 'odishaspeaks.com', 'saamana.com', 'mediawala.in', 'instantkhabar.com', 'hindusthansamachar.in', 'evivek.com', 'ncrkhabar.co.in', 'journalismtoday.in', 'live7tv.com', 'kamalsandesh.org', 'indusscrolls.com', 'dopolitics.in', 'lokpaksh.com', 'bhadainimirror.com', 'journomirror.com', 'thefollowup.in', 'myindiamyglory.com', 'maktoobmedia.com', 'thenortheasttoday.com', 'bartamanpatrika.com', 'mediyaan.com', 'rajnitinews.com', 'politicswala.com', 'risingkashmir.com', 'babushahi.com', 'asianage.com', 'muslimmirror.com', 'raigadtimes.co.in', 'dossiertimes.com', 'pratidintime.com', 'samvadaworld.com', 'lokbharat.in', 'khabarstreet.com', 'northeasttoday.in', 'falanadikhana.com', 'dinaseval.com', 'aadarshhimachal.com', 'tehelka.com', 'thekashmirwalla.com', 'mangalorean.com', 'tribunehindi.com', 'veekshanam.com', 'performindia.com', 'thelede.in', 'jananesan.com', 'kailashonline.in', 'indiantimetv.com', 'balliakhabar.com', 'dainikreporters.com', 'topstory.online', 'firstindia.co.in', 'namokhabar.in', 'garhwalpost.in', 'easternmirrornagaland.com', 'millattimes.com', 'khabaribhaiya.com', 'impactvoice.news', 'nationalinterest.org', 'starofmysore.com', 'livehindustan.com', 'time8.in', 'hindustantimes.com', 'thecommunemag.com', 'dharmadispatch.in', 'hindupost.in', 'newindianexpress.com', 'toi.in', 'bharatsamachartv.in', 'pragativadi.com', 'politicalwire.in', 'vyavasthadarpan.com', 'thedemocraticmirror.com', 'intoday.in', 'jansatta.com', 'nbt.in', 'ptinews.com', 'deshgujarat.com', 'dainik-b.in', 'chandankesari.com', 'bolbhidu.com', 'swarajyamag.com', 'goachronicle.com', 'thereports.in', 'absoluteindiahindi.com', 'newsd.in', 'indinewsline.com', 'indiaaheadnews.com', 'gaonconnection.com', 'thedialogue.co.in', 'rajdhaniupdate.com', 'newsspeak.in', 'navhindtimes.in', 'naanugauri.com', 'newsroom9.com', 'bharatpostlive.com', 'nationalistbharat.com', 'amritvarshanews.in', 'intdy.in', 'khabarbihar.in', 'angindianews.com', 'theruralpress.in', 'dailyhunt.in', 'goemkarponn.com', 'primetvgoa.com', 'rashtramat.com', 'divya-b.in', 'gujaratsamachar.com', 'sanmarglive.com', 'royalbulletin.in', 'lokmatnews.in', 'millenniumpost.in', 'bnnbharat.com', 'aslibharat.com', 'jantakareporter.com', 'boltahindustan.in', 'indiantime.in', 'ipnnews.com', 'mtonline.in', 'rajyavarta.com', 'muktpeeth.com', 'vkonline.in', 'vijayavani.net', 'varthabharati.in', 'pratidhvani.com', 'indianthinks.com', 'maharashtradesha.com', 'maharashtrabreaking.com', 'a2z-news.com', 'janchowk.com', 'chhatrashakti.in', 'thekashmirmonitor.net', 'gitapress.org', 'thewall.in', 'jharkhandnewsroom.com', 'jabalpurdarpan.in', 'indiaspend.com', 'newbuzzindia.com', 'thecitizen.in', 'prahaar.in', 'tarunbharat.net', 'thedailyswitch.com', 'mumbainagri.com', 'newspolitics.in', 'indiatomorrow.net', 'boltaup.com', 'dawn.com', 'peoplessamachar.in', 'chamaktaaina.com', 'jkpost.in', 'thanthitv.com', 'desiyamurasu.com', 'counterview.net', 'epw.in', 'vibesofindia.com', 'joshhosh.com', 'janamtv.com', 'ddnewsgujarati.com', 'indiacsr.in', 'maharashtratoday.co.in', 'neopolitico.com', 'myind.net', 'thefederal.com', 'janmantra.com']

for d in unfinished:
    folderpath = "/data/shruti/ONR/big_data/facebook_linkposts_redo2/" + d + "/"
    if not os.path.exists(folderpath):
                os.makedirs(folderpath)


    for m in range(1, 13):
        month = "{0:0=2d}".format(m)
        newfile = folderpath + "2021_" + month + ".json"
        with open(newfile, "w") as jfile:
            for dt in range(1, higher_date[m]+1):
                lower_date = "2021-" + month + "-"+"{0:0=2d}".format(dt)
                upper_date = "2021-" + month + "-"+"{0:0=2d}".format(dt+1)
                q = 'https://api.crowdtangle.com/links?token=IOspkA7AKgiNqZPvmhGDJ7xxSJWYYErUtZCUiBOh&link=' + d + '&startDate=' + lower_date + '&endDate=' + upper_date + '&platforms=facebook&sortBy=total_interactions&count=1000'
                
                
                try:
                    response = requests.get(q)
                    q_respose = json.loads(response.content.decode('utf-8'))
                    postcontainer  = q_respose['result']['posts']
                except:
                    logstr = d + "_" + lower_date + "_" + upper_date + ":fail"
                    logging.info(logstr)
                    postcontainer = []

                if len(postcontainer) > 0:
                    for p in postcontainer:
                        json.dump(p, jfile)
                        #jfile.write(p)
                        jfile.write("\n")
                        
                        
