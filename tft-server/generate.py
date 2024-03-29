from __future__ import annotations

import datetime
import pytz
from faker import Faker
from log_entry import LogEntry
from schema_registry import SchemaRegistry
from message_broker import MessageBroker

fake = Faker()
import time


def get_puuid_list():
    return [
        "DkniIOKxMKBPg07Bu04ouh23RjdDNhm6jfH3eWCDq-AHZ1rfLfFAODeCflQ9bSQ_IEENtDsvLCts_g",
        "uMQfvUqB71AxOEpFsqQ9uyCPSPu6jfutUUjMBcu1Xk8ZL3MLJbScZKjoTKjKDb8OXEqn8bmB1X-hyg",
        "Q46FIBYaE6qDtpyxjtMsgs_tVGF-6fH20PrdxaPtGq6EDFVUC6VdSevCEiH7SGvEyU7NfAlHhKls5A",
        "Y37DmanN368v8oLQeDI6rmNomT2wMHDZ8_crZCdrGwISxcqkQO8IAJowzi5TWyWCd6Un6SM5CVwoyg",
        "q6qnjarD9KI9PPcnV3nEe0xCvxcefP4hqjp5vg-0CM_cqbOTqYLDdNKkwFyLKUnmAERkPpaY6jLR7A",
        "-CxAYYbPP201sLHuruEzM15ERdGw09Rdfek8Y6rAWFGIrEBkqTY92RG9aY-GB3NuUYIQGdQv5mpdtA",
        "Qf5Adxs53qhLvJb3jmCwOaSXsa2WRokW4hlioq07Hi6itM9bRP0zzvo-FBwuO8tGBoeJYMg1sQbhhg",
        "F6v1JXt4RRIP2BYyaESFGcEaTIBcoK09PW5lf9pliH5y2B-r-b12Iyo9_46sjhz2p92CEdywwcO1dQ",
        "0JqUHVZo_Nf8YtIlQnpck4O1EjzReQqMmYwg6uGZrgBV4CnJgXHbAHVeMztXA15qByjwb93zHS6Zsw",
        "OYd6QGMQKOkmo8MRUyyvGbBlZkL650WNh0Lr4Wc-6LEJVC7legurbAQ6ccLsGXouz03oM_jnI0in0A",
        "DNuOAZvQYpY3Xp08cJNV0h2yhF_ZKB_P6h0i1Ap43aP689nzLF9fXFyseHof2zH8501NKviTxpbiYA",
        "GsEX6UIT2zXAODgkdtuDTWrFy_9u7krZvZAZsZbI6Mi0VOShTlVE_uYPXabPLeWCaVBl9b4RknE7GQ",
        "YVjnhvm7TR2LSCEUDnAkPIC2S63p08Nw4M7oCScZDdOoiz2FLAxkLkzRtpyJ0viecDf8k0qJgeN9pg",
        "qd-jAsueHAmmWDXZhISbyaBxwuWQUpUeV2qYA_glsvSU7pihLrOwie4tUgT6iYar7C4Jr8vgQ3Akkw",
        "MkEo4eM7AYsOQJIZZxir5e1Sy-kJOqTiGWoken7q7DAc518DPpKVAIt2J9TNYSP8-Uaykw38JC-5Uw",
        "d12LBXwoKJS387qoveTTJjFrwCzlFTYapXj2kYPncj5SzFEhOZ5AYUZ2ZRa0AkbzHIUBMGmvqsi2Iw",
        "u3e6zZXbuyjFVq2xnn1Sn-4Kg4MQK-sKtmk0xWF3coMCsYvtwXZskCz_6CBj5ApxufWSvtlUezwI9Q",
        "FgJd9r5Nv2zYM1G0YI8GVrj7rZEWaIoh7sfxKvLHxws2BD02Slw-gzHMBrEq9WE3wL3KQ2ogRGAvgg",
        "J8N_pPlcoEnt25sBXT7E_fbDHQGu6_QJtDtXiqocHwezD6AgvL7I0q1d0KMnpDf6_im2R-eP9OJ0Ug",
        "wWamBgJw6FDc5ol3pxBk-7VQ9hYj7889lifELPqPEKnIrhSOZtIMk52cgbti6J2A8QtqyMw64e78QA",
        "Jp2ABy9_vr4lxJZ8C7olwTig-iAW3V4xb0PBmew_shR9diIeJ2p1z3Di0VwFT3BpWeDa9Z26ZprM8Q",
        "wt_TkTwkaIZhOid3PFlnTDiTXrx5kylZKc30XDK65PhTDT9Qy0HSYp_TOjSEu_AYo7WncQXX-c2-tA",
        "G-t_pjDeTWKEYqPat7GVps_5JLX2zJF4bgobsphDz4S8cgQDXY_BF_2cCrD6or1XBFnrLUiJ_blgyg",
        "ou_p-PU4HZxWos4d5fXR-34qHDMUhht2gGw8C2ExISD-u4ch2Vet51mTSylsdwUgsjYj2yJ2bUf9jQ",
        "V536Gx0heRpm8OW2pjdN7jrB2V81so9tSeWmTTuKHdgvrw37tkSh2jTjld5SVSuP6Pl8RbxTUgHqeg",
        "duTK3-rAIRhz2q-lK7pv9cPXlbU42wYtmsjE2jSFN3FPr3JWZu8VqYRJrlBemJf3yyyM9sT2K7fr4w",
        "8LeG5salVqegf_QOtwEbFfRiOn35mt2ZOoAqAXKF7CQeR-Zj9u2BS1eTKJelh8FrCzbh6DdUkPuRcQ",
        "74wjow9xNjBrZySzWWNt9W6ZdzZ6pCoHe4PH4BHZTQzepM3qKanBhfsJ4qVVBqwgeUbo4z3xFGdSUw",
        "_w6uQD_5uCdBFRkyA3mADJNfNn0zQUUEMfB_9Ps7iapZkLnQ9txSyYzJMbiFTbemwBO9D9jSJgWw3Q",
        "IQSnZILvrFVL3h5qWOqxhqy8nKxJEyAWce45XW0CK3hCn6DJcJ4Pb5qGixCOMHe8UO2b9S2MEEQGHw",
        "O1r5UQeGCfSaBTLCQvife1EIfcUVAlH3AT6_2zUBPtxW03hJLdGNOc9wl39aTQdaHnSDSkncAJCiMA",
        "ByKQoqEM8lYEVAC9arEyQYnZ3aTtwJ44WLKlIvxcwFTbFt3RyTSpf-MMTrgvMLQuCXCF9Ng6XVyJ0g",
        "ognRA-an77sg2212v8CwGCElBCHFPPl_XkyyYGZaSM3ZlAvOLankNDq4hYRKJMVLvC_7CsoXvacgQw",
        "bXzKIAd_vJj-HViRPkD3cZOeRrqXRACsmdi2luOt1VCfSoewEibaPOIrSuvk-wPztD7vovYikDkJlA",
        "kuJ6rzcsKhSmvInJfHWbx2PJNI8ONvGU6IUbWbrodFnAiEyYi3eIK8BH7oVsdUg8YtP5bNIf_5j1Sg",
        "mbCwzIaPM50GqIjdpfH1EEKf9aHZpHJo7gmkIk5FtIeUQnqDlMSbQ4wR2TgJtEY-SbLtZPuEf5ZnVw",
        "U8pPUjpszq5FrN9LyVbYk3YW8wFj9wMDjxnVLHlE6La8RdNTiGAd_2dAebYQEy_pX32Zj_HUms9ezA",
        "LGgpa-gWTiYyjRW-9prruro2rGcGpPV1hFrQvbbGB2rxIH62QBNlTfzmsAcSKI8K5913E1EpgwSngg",
        "Lx8777JR77yN50OlZ0_e0KpYXlvM4lYbNIE-kLteA4cvZ-QwAAvxvix81IQKc1UQZ5r43maReh7lQQ",
        "Hl97orCkbKX1wBSe-qPlBU5xw4IaC7e1JitSPl8fSBvKrFvzKCT_vj_OSQ37MJ2e8UCniT0Qiw76iA",
        "NiICUbIIact7VEJzvmTr4U8lScWtNxUIKfTtzfBia50gHnEt-fVz9IqKxPS2kAHruQ3EDQ48zY6Fww",
        "toaLE_mxQQc8zgo9t05oZSF-K3tC7I_oYgk76zC9BDql29ivOsDU6F8hYfbqGLpWYTmJQlBxkg9HGw",
        "1s9DOsW6sY0FeM7cFZIq_eVxvltH7wPjpv6OqeO6V3FzOY7rTIqSvD8tbrzUjATxO4Su3WeO0BUYcg",
        "qc9f5hAjP7SbzOX4az1vRfegZzszxqUXo-HsE3e1Wz5htGh0mRQ7R5loRp8hZE0Xco8WxE-Kk4EZSA",
        "ywRXGVwiokhl0BICb5PdYhMYShI9ACyHyDY0jz4GtzjRBu4OkgZV-nUIWGuVfYphi6kyxPFVqIHG6A",
        "cSRFzO8j_5ggjZ3uyCRQfsKFnbS68CgJYJGEhvKGqvnAYpX8Eo83qg9EJMlEBSah5unw-7b5bTQ0HQ",
        "10hZmOBi1XTlfZLVdtT78-vKj2Izv2b5dEWVo6jnSjm-jtyetc5yYFevR2GojmUqI2AkcyuVg1gaGw",
        "W18zBWORQ1odUiuXSCVwrb_knpCwZonRe2nRSjrAZv3mwcNnXKlWMIsZ6sTjX_Xx1tGPbitfr6g3tw",
        "GUAr3n8jshgInGmVzJhlFgvWPqiAepPzoouuhCBUde8vHxXDYSA12t4_UwBll4LvANCxaOTi1RsRTQ",
        "ZpWUDsVSY3iMUjElpux4HKcDfRgbUwj4URPPKMp0ZS-3BDLoMHHcFS1f4kx5SKWHxTgf27FXKsbFIQ",
        "rhIA3fy1eQBmHX8Oemo4R63OMaWsdPri84r3XcQn7yPqs-4zfoKI7wVEsBUZoP1Bz1TIXhxcsodTwQ",
        "fms5CcLwxU5eO0cgg95zIQk0lgtxVxpRRDPP6WWUNtQwc5a8va74etmoS2GjED4kiEL5_uNXXjExFw",
        "gqYW71J9ZdEp33lMXa7miDOX6bIoYPLLEjnnc1xz-pzqb7iH5w0LX-UL3zXWoeE8bJfPR1cdf9U4SQ",
        "7m0Zwg3PZbcFKMHTaRCSh2IaHD2L0Dv4LpIW4D501TaLuBgNUslAlhFOnPZ7s5k5ZY9WrjrUR0V-cg",
        "QChbVm01kHgylfc7LQ1HK900X2QbCXpBRkf60Youl9YOnV9D4YHW64yMR4YTYJZMirteBJiAVnbbug",
        "8a8YMgqB2iYlCodJkqAey_Jf9Z2Kx2xny0iBqsG-o7VF-wZz9VFWQE7Q0D8hGwPEEHHHnOl6fL-kZg",
        "tw3ZzFBWUGnGd_yD6SMmeVVU2h11EFI8BkXogtcEmPwNVYLVaSM77olv6t0W7r3CBPpDMNhy63NpQQ",
        "RojnCGTgcK2dOiJv8zSqhR6AmF_m_46G5KP72wh_6MNIM854sq4Ay7haurwsD3qddVLnTJWrWyAA_A",
        "gD70drCk4LYOHlZZ-peNSA6e-IUs2FFjYGg8-n0CZSs14fvWUC5fbqRVSQpSA5ysXw7teSvq31c-rQ",
        "UCCKpVlQSdOS3lMLY4vyO1ygJE3otZw4CKAXy5ERy-HQUEFwCzPAJFvU1rep6q3qj-AxZ2iaONt-lg",
        "i9_kP8TW7R7ckaE0_hIWkT8B4RhuXZPQvy0Hd9_7ABR-NtLPHt0CZPcnWMbky_EKssEwK2DWc_o7AA",
        "m1BARIM4vQaC1gjsnzNqF7WAsS_US9861zvO3h4zkPa2TBvUql9pQSU2z-FLsfigCU9j63_0m4Dgaw",
        "xFTa6YJNFA6ElyY3kv8SkBNkawsC1iYwdLwkjM7j1NaPcQq13lFe5ZOyAE-lbs7ofzfweDk8SnmKwQ",
        "iyh4KibkqHmCfwg7ppNPF6LuQYU9E98YfmbZFjUfa17VLfSDXgtiodI268SeAaLMjkL3gEJv6a93vQ",
        "Qyr5oZx2IhqsADPtxqV94tABttfn3hBQx6UpA-aGepR8Ac_puL7vsUz4z5QLwkpAiAHSEyXjf9v5Dg",
        "S2QVXML9gXkeNyMmg9VW4nsPRDGt0gZmIg1Bs0tI93t84ZEmTgArJ3wijaNmyvGdARtcUqN3i8MZ-A",
        "eWDzUNzU5f6iCtNJ9db9InSM2yc0sBjNYVuMZnoPGFCl-wJH1oFgFhxY_HVGbh6sGcNfLtlRTZT7DA",
        "ajCjmKJnAUDnP8OhpPDEAwd3vc37cupN33d89JXIuPCT6akvOFA7lNvBpe0mkCuu0YNz4RdSOUyn7Q",
        "BJzBEtlJxpG-rkkb5xVcMgyb8jzyoacc_lgYJwUEpsNZoIiJJgXiyc74POJWlDmciHz4tL1LYCLUFw",
        "afNJqToaSr7zwXBlslyNUnY8jOvJM1QqAx5YcQkoH1WWgVxXigbLivZdhya2Y2A1LfjqjAWyEu7EvA",
        "NeWdhHMFVFrSc4Q4QoadON9G5usn4QuN6Zrdp7lTCSPdO6wqDoBRUPUX1WtP-wLOQSfEOjUkdCTJoQ",
        "YE45nGxgGlznjqkvxS3Jk9kOpzY2IbtgtbwWStk3usGe_NY2impnzxP5UXQtOBHo6t_TVVeelOam3A",
        "HsOJ8hwCb1MroYh1m-kCs9mDFK13Df1MXA3z6Vj4fnOXwA0Aej61wHQXBEhIuYzuZ6Bir83nETNAyQ",
        "69LIBQOjOR-HRpzPoCUnj0JNc2In3c_yxLMUWQH3McunD-WpfHDJVoSKjRT_1zI-nK15op25gBBF6g",
        "B40wCTPW39FmDOcnUR1nXM0gAI_WZVIPEsdCJK22cpN7GpJO29kIwMxd255t1Kt4yF9tWlMg9v9vrw",
        "W3Cpw-_IzkbHWzhXcu1Sfq6ZtCaZpPy8gqYknyXB5GIs3Ayhf7pamb8GoBz1llroDmQ-wr5wnYp5jg",
        "nJaeTyQliqYgXfWUgxaHs4gj9yMvjONLXLu1fTM6pmOG_hUJ85cKrvfwpSw0MvSjmqjhZS9t9eCmEg",
        "z3trnv7UXlZOGLVmjFueuTyhpTBF61EKRPvjkvxWv5VExWLEWn0Duf_NCE5aUIRwMPna6q9CMkd24w",
        "HX9HbjazwiEOaX5KkKtsZDiFKGtTddkiRBzsN7M-1lx39A0z_h_qlaVs3niHF6xIUoX73BzwzMakAg",
        "epZCQA2LYlhet0HRDK0MIVq29RFkij6JoimfPRqLOg5vxOAmehubEfgfsnn03DJh9HOpMIkyFW0sbg",
        "mA9ctMD5i8HIW6t3Ivsa41vTDY6hVnvv1kYp4SXQ-YdmihSwv5gQhDNUq8SJU2mVPbbFk081vYHM9w",
        "K5MksqLtrGPNjM54R09nzxhgVuZG577OazQ4yCuXzhfpya1FAhP7IQFnKKeGJ8vadmDsFVsnE_CLig",
        "HCd6s6FsrXSSlwsJGdAELIIoKwd9ZDqD13O_bLMw9HxUIRkrFyaWwQaQGlyQ851Lz2xG0u_y0X_Z0A",
        "LjHsaO-jSzKJuI32S-3nYX3AyugzeoLvhraMQDdmt3OQJXkKLiVIA1vTgwN-N1YlbUuXORr3XsrJHw",
        "LEbEg-sgaypqoBsxGcjGGi9PbSnyPK9pHL_MT3OXQsUq8eHnca6J5Hi3i51PySzQtBLTV9dcXIswhw",
        "sRyDvWlMnZPE2nMjrpu79Zt_56PkDFZokgUblC4uUcVP7BsGOE3SxV_gliDnfkMW5ytspeiPixiFYw",
        "xOwdz_Z8XYVOtLExiL4ub9VpQb6tM8XAkX_R6iRruz_-fkTn-HxcryB6ouYPWO8fbiy3GgScQMCUZA",
        "-_Y99s9a2gQaxBKfjdvNhs5cUTS9NL5gr6t47oqQBH5PzMhdzSry5tNL9Pi_IFPJzShhKorbVTZYxQ",
        "RuDAm36Q--AmR4SUBr_JJWXX3qWIdDOpOZsbfZWX_P2uSxiupxDvlrimHJ7t8uZzt2zlRc8SRyOkpg",
        "OMdXA3NksRZGbygaVAuglN9dmySE_gpO9hj3mqakMYa013dI3bBWUWQnsnXYyO7r_UpqCfkSfaVgFQ",
        "ZtbLtVeuZKiF_sOm1e-uKWuX0-DcmFElDNsd_0qbfOYklD45TGQnXOyYYktcL0U7dOM_dWh5fvlbUw",
        "86cFP2RIJ1VQhDg7FGY8RS6PEpi084_2glSS99LO85TQVtUlGLtg93VeWqparmxOzXg6oz3fWtGAWg",
        "q6E4kEWH7OcK_YxVkDVoDQlhdO67n5hLDZnBjbehNvjLIhDsRuS7DXxcjuvkGbSNrGgG2Dpj3ACtpQ",
        "3wF4NK16ieWTVVUHbxZjrnI8RAtRk0iQAR6IzII0yO96rHzHUy_RZ0e_MmlMMSKEjl8L7uNVt7z_8g",
        "zgyiOA1nqGlEN35zps30ygBpLqfHBVxSaEzb39MdK32vvc1UzMu57MCfSRoQXixY4haMIkPkkQbiwg",
        "h4bxd3oPfPYF0iX_qPcHrPaKwa9ptEKh1VvpsPKdnvSm6y2dNKkCPjqle5n0sfzki7y4bu0iuU5S8A",
        "8wBhmfaWnFAtpDAl4xQwqcN4O3_zJkT3JlwviCPgmj1xRWVbuBHGtxZw8bBGE7P62onFcrJdM7YShw",
        "xm_XZXv391o1zBN06yXXKcMs9iw5lqLX_M3bzlpqP8mGkKkS6Nn9ZMmCv37jpM0nePCvmFH_6TGQ_Q",
        "2qwaFxT8Dmz9yvWJqVXCZKOyE0kqZWXu16E0T9LTJ30a41nZRyjUKwtwXuAgLKtWEd8et9cQvX0ymA",
        "XFrDIm91WfUYJP1zZ2KG45gvOfc_y5rM9_zSoIFxuLvHxTVLLD66tNsXMVoF5U21n8334GFwjQGXUw",
        "BQJvyeetNIamZW_VN849jA7W3G0hj27GbPavy0nc2K9NSa6XbQHfRteGQ28MoyscWtBj3LK6C7uYlw",
        "f8tbJvHrX6xN-8MECMWSh9vs33fgC7CkBPyi6nFdcBWBhzVbtoD_I47CqiRs-koU-_JPXaAQar-Bkw",
        "AP_EJfBV5a3pl6opGxPYDAtYBJtpk0GdVqU44hRX_Df82Ai08I9Lt6Q1I4eWnTV-JK_x36z9qxoXPA",
        "GFVbcn2H1hqcCo0G7lct4xjuVn-8IDDEvQ6C1-_f45SGkMvje6omdge4keooprp56eQiUzhxvS28ZA",
        "bS3xXJISNKdIAv_ZLVwFN1rzNlIAJoLL4WsMaZyR_ofKV6vsmLn7efOxHof7nHk62qMtMBRvemxZfA",
        "-eRK2BNSW4XWDlMvNpz_SZNgOMd-eLUzYJQymaqcoWHFUNiNzRJg37TNck3f89I3Ty7A-HRLVVoRpw",
        "ZVWup1aHVod7X5YT2aK297N_aIyeGrYE06CgcQh5Gdg_CLfQCunWQZuQQ_w77jVdm8px5d47GjuLdw",
        "1hWbP6TIbABEMyBqVa-lazIcc11Ez3wgTqtlqvxB3vz5L02VL72jT128WykHEt1ZyIoH421AQ4lNYA",
        "B3Nx30zln7Vwc7CdkQFzFPQqhNN-QKKSO0dQUd3WbKyJ-2wPSZxDan2bAe_hDso2LPwT8P89r6KVHw",
        "auX0HVzIUzvvGJuysQqy_elBWAygKeVo-VePKJjkSux_wYNFl-Yr11-ahdffWnpPJSvB7l82KBAFtg",
        "MFUKUqvdBEFIxHFZ8dLcNr_kRv4Tn_zKhtbzB9pRukTt-51R4y1Rk65wQX8-vwAMlL04krHADP_Dpg",
        "HVkJUGs4ArlNKcUCt0ifxBXOkO3DFERE3F8yIG-kYha6rJR51R0AvdEM_PZmog0WM9OSU71z7LQuJA",
        "tCIAm0Chetyf_EM9tqNh-OHzSjT6HGedc9KFUEPmDdiRsenO1QtX6xeLr6caj7dopbNOx2wh7aOoaA",
        "GC-TK6BYui0X7iy7EChSMseDemAvFCVn7RuqsbiUuAvzHrQhpY_CCYXGAQDQWagZGyuVdr4kb0h_0Q",
        "G7u2IdOfuQnM1ATeViqMg2D_arQVlbjhD-aOGLesA1h-17HriNNtfWGO89BXJKrDx3oPnAI4qjmfPQ",
        "0Ruc6g23gMPg4OATIJywp3QBXwnKstiMQBSUJNi5hRwgYMXZX1EqNj3x2FrEWTCQ9a338lthRK4EKQ",
        "rbW8eEaHGF7prpp9-KVIc-j0Rrl6zwlAOhh2IrIUjo-D3H7snre5lUH84Y7nKcQtgHdJBNUgw_TIyw",
        "yrNgRIcCS9m2Wc3NNkoBik6leeSi_f6AZIRJ8sPDK-vLN2uKOuT-Yd3Eq53PydTj3CSsnEuvlC6Ohw",
        "S6iTraoWSOJm6GB1drCMHN90gEpfKbPGBcxTwIFY7W_J6YdFqt__o9-Li9L-KcoXRhEySjgVVTQlhA",
        "TBCeWC6q-OpsD5Z3oSV94ogh1AP5oT72zT3PEmnxj5NjiggDY3Un2j3jSPlpcVuI2MMOpk4LcnxFMg",
        "4wzvRaYOHAJTv46J3razlBh6-xyQfWMxIIsBS9Yg2-jX3E7XDmqzoZUw9u7bz-trhd6Y_3y4mjUBDQ",
        "IjclVhytNVP7vK1sYfr7hXcdQWinoR6CSGLJvvw4mhXM7KgEro4kmubRb08KY6Kn8e5nO0bHCM8KRw",
        "y8yveWO-iNkcx1G3lSLtiKam2nPrSz7dubS7ua4I1G-kRjss8ZoD7CL8v6PxXkyEmgdxVPB0VktrAg",
        "Y7gO6RGoXGWTG94lzALbaQ8DDG0rf-jrYPQCCakMnDcV8bFDk2wQIF-j6QS8TCn66p2aDGbmRc8rHw",
        "U5rWKHzH1FTflNVji0XSC-L3_EfQNf00zF9bAx0plSK9NuJJmMI05tv-2OwIaoL2HvHDZ0r35zFZSA",
        "81YTJ8tqb-tObsvQc_Wf0oz_wB5ZaRmL9_i4ouuu-urGVEWLo3Eicbk9aB0rD8VegmqhJJBNia3lWw",
        "41-ge5vjCIqfQ1mlEjQn9KtN279J8IVwL52qY7rgkGahWqNAUpv_CXRSTOb50eAagKjN3zZZCIk9DQ",
        "rOYx5-_l8a9fOKYq7mJ1hTl5ENM1N_j4-8QAcnBzJyh8Uhk4D7NY9j6Z5B5KFQv-EeQ8Kp_Kfm2wPQ",
        "QENHTbKJKekCqR71CUgTquxXY-LC2pTHyqxKplC1bJq7QoAuQCI4yez30g4nNeZX85_UKXy5OvK7WA",
        "oM331rIfCqWqfqSFTlsWfsO408RpdtMcPPr413kA6lcaB__P05UeLrm8wsv3iaUMpzEN9-j0osbqrg",
        "wNHYdDBzsnhonR083z6lhk0LkRD_9jPQ4a4BWu4HAfzEq0VSjMqH5IhljOSsWVIBFjnpABtpHZ_fmQ",
        "kusL7-9jYthPuM_EFm5d0135KrAWTDwAVXCvuz7lV6hFsskYwqXoUYtUc1Q6sdPSD_II_v0RJiLY8g",
        "nSLMp3iPyd3rf95XJerbLnUy-vr4K_LhVvIAkFfvJvBRd8a87iDlHn2UIbYW1MmuDSJv7wMCpfidAw",
        "8PzkMrPauNiG-ifiGXU8qxGMwyrz4o41Kuc97tZagfAlM-3WlAq2DDVtzloaQUy__jnS6bix0suEKA",
        "b2EwTip5YRTBhjWXDuF9vLj7-6fSTdWShgG3Q8TNYEFxb32IDZ-xBCSRjz8lA_XjSDSs2F7UzCNGiA",
        "ifWlor5rNVa9NF7Hdwawz0O_9dZt9u-P69tbSRANMnuHGrFyspBQjah9Oqj2f7SQwffAeg9d-fPvpw",
        "EYKMWhPfYWODBVk-pT1nf_Xt41xfDZTK3_bRvzXUZYEh0usK-AWk6EGrGu8o5G2mzczlAULDsgP6MQ",
        "2pHh77e2j2jrBH4mYSReJ7COjeLFLGUFumgVzZEnxF__MtJxVgZwEr5MnIOmjL3oEf-ObCy7NBgCSQ",
        "ADM7UzYnUsMu9SdYEahhIZwlTImtFfmtCWqIoH7ErxnqfCkc-FLMoh8VulMf6bXXgj1BI1hR51K2pg",
        "l4ajGHfd3yIxvTLOre9wS8fPjLczPWSsBnM15pWRkE3oejv9j3A4c72AYO5a2olYN8ztJ4EyVo4anw",
        "bRBPLmk__9z0MtcDzEy5blr3qTRUjz_rFxfeZqbOPHMHtHpPSwilzvjxmDt_toaeqGuJfKSGP3pjZA",
        "Z7-ey4G8JGj-ruX72oqNruv6cPoMl7wBYIgL-J5lzocRydmG__Uw16PnZj8eg6VYfuUSYeJqVSr89Q",
        "kLL5x3VMf60G38yIhR5hS1_RBQpqjpzbmXFmVBKNSgCzuLNpbCzJPqjn5OImSg0zF7TkDGi5kLi5fA",
        "6NrB8pGJXfixzKs9BoizKM3ys5JfP9OHvV9Ekv-g6lnY1YOISzkkHNPfy0rk6p7UDaCB0jYWq4SKtg",
        "iSJo4NliYSmPFmZ1lxFCb7oF36IGC_G0Ia2ZVVmL_Ye7W3a3Dnx3CLFL_vcCiSb125m4UDfQ5hyYJQ",
        "SlD-fDpOb-suy2SQIlkGR5gMyDrsjFpOWpfnaY-uHj03XErbJtSe8q7U3liF4nBX8wIkxroKEt9WeQ",
        "nQjYwzhGHl8RQC9gP8ekXus_XGwgtlVZNz7z8bGwlAdcZzGwyTBziOdXKJt8tLgxL6PW3BxGXt9gow",
        "Sa8ae9waFiEOTevsqUAZ9LMldPgOebO2xobJ-zSPr7at2XZuaoLGgchs-nl_f4REBG9Sjvd1fu53sw",
        "W5o-EwIR8P5ukoLWEQAaBcATiJmS87OUCf1s4ypg69-3wfsQ7wfzw9h02z6e_JNvZAggoeUbMCNB5w",
        "4SUrU2Lfi4Kav7qjGIL-wqTMyH0anIqDtgzlJFf_hMeHQGT-XudZqmNbapjm2SBOoq9ZIzgWmkI_Bw",
        "-mT0Pg8L0v3xsi4SeJHhsUxTzetKHw0fa8BHmCeKRT6vJHK2yBjbRLniG2oJgI4nlc8pzQogZ3OoEw",
        "85l3elHmgZs_Gl43xFARC-eAyULNlHVM-KpdvEkwHQd0-7MfgfQdvAFyZwM6a2laCC7B5wmWWnyuyA",
        "Oobo1XnVNEI8q6ygu9vAo0eCq_cAmWJeoKEl11uwpj5XTKRn_cNa2YwJYTOfX6ytiLnpI07segV-Iw",
        "2VoLN0-Wnw1XdfQBpFs8yL8YLjHmjhaLEmBvUk2nqETiNPAuVyxA0ARYTgLcNBhaImEDr2kPUHfLQw",
        "CCYi8d5ub6a3Ug26Fi_rhh22E07t9KxoHDGMuHHz8Z2dZA7o941jQMEMqHbVrCxrFWamaOT1-oYc3Q",
        "SpkDY_aORFq2bqvz6RWa1fr7-qUZHgOM_oduFgSGsXguCgc3vjUstyUNmgiYrqZJ9xNMQpj4-4T7Fw",
        "smLEl5LOtyaX3nrasU1tJWQwRvuj9DZx2cHqCThVSeDT-J5Nhj3Nkyn3Tdb5YlwXkmWW8KXYfBLOdg",
        "sQpAUYg67M-dPWj32eB-14XInx0AEHLvWyy4jcpPpDnq87wbDykV86k97mcWv7riDV1g2nLsZflgHw",
        "-qOXCrb7Pi3VhnKWlcac2mlLJ5PgjIAKzfgsqVatoUsOjaId0wWivA4vU7P86WKEauXzChwKw9TEJw",
        "eEfQ1MtmJL8HuB4R3wXmKkIamxK9q95jQX2samcJvKfHED_2eeftsoWl0VtpJIJwRTaZ0QZyHDw-vw",
        "0ih5lr-XjqY9r3uSK0FjyBfvpTXu54CA5VvMarhlx3svdvxAkASkiiwkpFbiUgNRHS-xe-tkHyqwFA",
        "I6iQ6bP5dBHXCP7FgpQN9BfUN-ecB_lZcDJupD500Qlsx9onsotIqaeOl3HJzPiAJ3LAh6dXPN79ag",
        "ENAMyESPjWj-_xPGM8N68pNwoXVrtM_RuKk2vLSD19AsnTskmVSSJDlZzdnrUb5YR4Z1_O0LmQFbGA",
        "J6OcvKSlVEpbG7-nCGbNafZFVDW7PuHytigr7ov1O6deVqakILfsmCnsn8eNO4r1tiF53If53XQRqQ",
        "NT8mNAwscoUBA1nPpAQ_ynAVOEq5fWsTXRCKJPhrzuerjOOLRCw7K8tWNkyCQXtZ62v7a0Tj9m5kHQ",
        "rhBqqia0kddTsjLl47wRGdSGNB_QFV-ez8P1sIfgz69s4MkUeiTSO4O7bRlP85NM4KZuFzrlY-mI-w",
        "YApXmoa-Mczvp8iyXuhZiBosL_EU_CLCzQFk_wSzfbxdZsSa27jtpWtylF504n-ApjzHVLkjP5SEWg",
        "HZcAnWshUtgECgBkUtNaSiZ0LX6Wt3KJ3Rw9GYpyp7b-8pBLwY8zLfM6qepihBuQzhjbbtxYY7TfPg",
        "BAVH0MYSNBp-tBdxAoFCscO7v9RQjc-YvakFs07DJs8I3P4ajb9ZzkmcnBXYLubcR8Q7HQmesL8P1w",
        "mWAp2o15eQ6E8_1psjZCRv_2jKE4wRFo6uMle8i3rvhz1ho7aVK5h4j6LqUp11Z8ObYFwx_owzEZLg",
        "rLy4hyVP2ayGC0ofsk6RiBW4XvtWfqSI46McZXitUtprcTPEqPpqZQlgw2p5uMjkTfTpXQDuriDgpw",
        "e_ruXBxnalDsQHECLq_S6FCZEyaK1SEbGUi-rqmHzrdw6itPWH8mqEEVqwju2e1lNDl5HptN1JfG1g",
        "gyoOcoShXX59LtH2G-rrCZc4jmqKs0x1hmaO2Xtd7u8KvZWu8_oS3XZeoQJCqpHPPilmesNX2-BMfg",
        "2KaX1pVMURZUuaUip9d8gN4Q9RqXKv2WXqwX0hxnHt7FTtszh3ZurpXKTL0hfsZlphI-zh7nxSAWHg",
        "OuuzoOPctikIXnoliwwKyTBq4S3fwJgqTMUgN15wUD5Feg0GRH5kFNb7hAoW2BLcsewcuC43Z8WffA",
        "zL6yJKtNlSCAbR9w3mnSDQoHm_0fIjaP0qAIn8M20SVCDes3YR4pPpAFN5poGMAX4fMv-UnAyQocoA",
        "2uFNOW4Qc3lwJtpTrsnj7lD6S5O7gxmMAw08UhNSy56z8af2ID2J1lTQiqmyKR2GF6WyAraRz0WXbw",
        "aLCNnH3jXMS-tBxN5X8ph0RZglwcfo8sQNqx7ZNP9wIFPjtlLOiPMc1b1RMYb_ujFrNdZ0fA8aaLOg",
        "keshA4an092k1oFjMq0j0xFV3EnUMDURfxjILIa3dTx-Nw3-aR59OPlHJBwUFY_IzXDc0SUgsIjA9w",
        "HIAEBeLuZDFd1mFcCzjYk0DTnaYUjGJO7HndHfK4YJZdtQn2mwYwtmQ7hC5aAleyWRuB0TB4UBLJrw",
        "ChErsY4IIlEKpgCHXhntzyy-Urn4NRcPw6941VS1xrUvW6C4aXTmCgmOLoJnPodwmbcWuAKCn5rBrA",
        "aWnhWG5qEj5N7eES6rhYtTzrsPjRVgWm-aw632cbRSPWHJtO6Xx8OU93QR-F-Q7v2JBzNiwD3CXKTQ",
        "dp4FB18xNW82_52VV2oT6f3iwLI8x7wuJJJyjZodH0-iHRbe_pri9uYRwnKqp-kFo1WHWP632ua1BA",
        "L31GOtviIQ527iuZVPwlTNV-vYewo_N_tH6JLCyXoTC6bUhHRnYzjY7oz9TRZp091YBzloDnKLFvAQ",
        "T710ujD23ENc10i-tO_sF2kyPUug2pj5Cz-53Q1b3O5UxQELssLRB-eMCXo75Er414PX6lDFf3fP8g",
        "pPGcNX_FocdZTTSuN_vcgDbaIMaAMsEauoG4qMgj2Dfgrvw_nAoPoe2_sr1obHZvUZuCGQZTc9Q8Wg",
        "A-Stig0WwSpuETDW-ehol7lZk_xXmcE175M9TgjrdkRBZmdJ95cbniVXOPRbjPuxRRoRglobQXKwwA",
        "Mt7LqWG-aHHyrY7aGiroGM_oe-hCiBYXXkx0BEWjpuHA0vF4BIPMa54KsRNLZjxQuQQ7Sfvnuw5Leg",
        "1wKyKqSgyQ-A_fwkTYaYkpnWkVJulAfNZQKCsnY7cQ9IlvsRWPMJ1v1e54h8BneRaUm2aMirfutMuQ",
        "tWkmrKkYvbXgBWXxJreLRuUk6bO2cr9VzwRnMBIfpd2FXNoj9xQy-difAPUa9LJdqZbjXl-GlQL_1A",
        "_drRqVXF5vpI9IFfXTH8gvSnuME15FmsHluI0NHxa-ECoW_mOFdPluPgtpEZdXok350arjtilWdlLA",
        "UuZI1jiO3pYzpIHgEHGANWv1d2GBGrzUN3tWoJBstrYgyGWpEI3vgoHyhU95ZlbyNRUUpWeQw9RbXw",
        "gIkg4QCx5rDsKzwC4dGRwNI72_EWX-Tr6JgfZsBaRJ7HBRSii21cbGhkztDsTDX2kAtBEcaW7j6s7Q",
        "yvpnLFUZZUn5P8NqvE6BEIYyldsfIJ_txA1j5tbgYJK8lS5Ffek867pgL3rA-y9aYyeKYFDbaJQoAQ",
        "V9epT05ZTeBRTPu7HZdn_NVzjWB1nyu0qgK15PbjN-6kMYx_JN7k9u6--NGlri6We7m6RRahM8VPkA",
        "YfEh5YzEzZurViLbIZUzX9JuZZslGVR5z2SglyQzqaYSpODWytANHIsWGYb0lCIJZN4B_ZqY4nJfkQ",
        "y1fuVS9C4oMEW7KbBJYseSz1IB654V7N36c9xoUcNbiwba9Fd_lhzGlFKeF0PX2LbZZkonUShtM8gA",
        "DIZdi4g8fYPBFBr9MHNUcNiY_yHWMzkQ5lFvZz1InTcpJQt8avLWr-p_A4rl2PIbdLKoDQzrAU68iw",
        "cx2kcTS2upOWmIexWbghV-BRSC2ceZPA0jFiV9-FNmNckbd9l-2rYjiKp2PvaTkNj9pLG39n2jV6kg",
        "06GycA7ETlYLUHcwzP4LHHN_CX9QcX35CjL1THy1CkAVePOc1SjnSviFZQ4aLt6uSgkX2mHuN5xyHA",
        "ki_pOw4PaPF3_JtAA8iFe71mSmBHPqvhv75nBNvCtta_URj6F2N_9ZFlespSUUrbggBfkmJhPP9MLQ",
        "l92GYMC430Hq5-SCmkyGFlWaYJLMhLqm209rxh3-z3Ni7_hUbEnfSaKOzI3ojo7dpi8AE2I7WXKWtA",
        "NbT0ChkwcNToY5Hg4F2V2JLj74-pF__mt1MbCRe4DyeHsxjbYc-b1wikFP0XJzBYoZNsa81k7oiLcg",
        "q_rDt5Fd7pOzHITsJHKOarDAcW3RfYzbaqQ5Atj3Jul6-okXeDrdih_NcDd1MA-LkuR2sJjWJq0zKA",
        "P_pvJHT7H1YJx77hkuW4c5iq2_3SPu_I0ZbvlC4S25PR7rsKgsEc0zqeC8mhSvM57E_lWB5VWi8uVw",
    ]


def generate_log_entry(
    start: datetime.datetime = datetime.datetime(2023, 10, 1, 0, 0, 0),
    end: datetime.datetime = datetime.datetime(2024, 2, 1, 0, 0, 0),
    timezone: str = "Europe/Belgrade",
) -> LogEntry:
    return LogEntry(
        puuid=fake.random_element(elements=get_puuid_list()),
        timestamp=fake.date_time_between(
            start_date=start,
            end_date=end,
            tzinfo=pytz.timezone(timezone),
        ),
        request_type=fake.random_element(
            elements=[
                "POST",
                "PUT",
                "OPTIONS",
                "HEAD",
                "PATCH",
                "CONNECT",
            ]
        ),
        endpoint_path=fake.random_element(
            elements=[
                "register",
                "game/start",
                "buy?product=battle-pass&rp=1500",
                "buy?product=chibi&rp=2000",
                "buy?product=egg&rp=800",
                "buy?product=tactician&rp=1000",
            ]
        ),
        status_code=fake.random_element(
            elements=[
                200,
                200,
                200,
                200,
                200,
                200,
                200,
                200,
                200,
                200,
                201,
                201,
                201,
                201,
                201,
                202,
                202,
                202,
                202,
                204,
                204,
                204,
                204,
                301,
                304,
                400,
                401,
                403,
                405,
                415,
                422,
                500,
                501,
                502,
                503,
                504,
            ]
        ),
        response_size=fake.random_int(min=0, max=10_000),
        response_time=fake.random_int(min=10, max=500),
    )


if __name__ == "__main__":
    num_entries = 10000
    start_datetime = datetime.datetime(2023, 10, 1, 0, 0, 0)
    end_datetime = datetime.datetime(2024, 2, 1, 0, 0, 0)
    topic = "tft.server.logs"
    print(
        f"\n- Generating {num_entries} log entries between "
        f"{start_datetime} and {end_datetime}\n"
    )

    Faker.seed(42)
    schema_registry = SchemaRegistry()
    schema_registry.register(f"{topic}-value")
    broker = MessageBroker(schema_registry)
    for _ in range(num_entries):
        broker.produce(generate_log_entry(), topic)
